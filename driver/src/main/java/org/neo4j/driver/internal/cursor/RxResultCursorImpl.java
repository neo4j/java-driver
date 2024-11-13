/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.internal.cursor;

import static org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.TransactionNestingException;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.GqlStatusError;
import org.neo4j.driver.internal.bolt.api.ResponseHandler;
import org.neo4j.driver.internal.bolt.api.summary.DiscardSummary;
import org.neo4j.driver.internal.bolt.api.summary.PullSummary;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.MetadataExtractor;
import org.neo4j.driver.summary.ResultSummary;

public class RxResultCursorImpl extends AbstractRecordStateResponseHandler implements RxResultCursor, ResponseHandler {
    private static final MetadataExtractor METADATA_EXTRACTOR = new MetadataExtractor("t_last");
    private static final ClientException IGNORED_ERROR = new ClientException(
            GqlStatusError.UNKNOWN.getStatus(),
            GqlStatusError.UNKNOWN.getStatusDescription("A message has been ignored during result streaming."),
            "N/A",
            "A message has been ignored during result streaming.",
            GqlStatusError.DIAGNOSTIC_RECORD,
            null);
    private static final Runnable NOOP_RUNNABLE = () -> {};
    private static final BiConsumer<Record, Throwable> NOOP_CONSUMER = (record, throwable) -> {};
    private static final RunSummary EMPTY_RUN_SUMMARY = new RunSummary() {
        @Override
        public long queryId() {
            return -1;
        }

        @Override
        public List<String> keys() {
            return List.of();
        }

        @Override
        public long resultAvailableAfter() {
            return -1;
        }
    };

    private final Logger log;
    private final BoltConnection boltConnection;
    private final Query query;
    private final RunSummary runSummary;
    private final Throwable runError;
    private final Consumer<DatabaseBookmark> bookmarkConsumer;
    private final Consumer<Throwable> throwableConsumer;
    private final Supplier<Throwable> interruptSupplier;
    private final boolean closeOnSummary;

    private final CompletableFuture<ResultSummary> summaryFuture = new CompletableFuture<>();
    private final CompletableFuture<Void> consumedFuture = new CompletableFuture<>();
    private final boolean legacyNotifications;

    private State state;
    private boolean discardPending;
    private boolean runErrorExposed;
    private boolean summaryExposed;

    // subscription
    private BiConsumer<Record, Throwable> recordConsumer;
    private long outstandingDemand;
    private boolean recordConsumerFinished;
    private boolean recordConsumerHadRequests;

    private PullSummary pullSummary;
    private DiscardSummary discardSummary;
    private Throwable error;
    private boolean interrupted;

    private enum State {
        READY,
        STREAMING,
        DISCARDING,
        FAILED,
        SUCCEEDED
    }

    public RxResultCursorImpl(
            BoltConnection boltConnection,
            Query query,
            RunSummary runSummary,
            Throwable runError,
            Consumer<DatabaseBookmark> bookmarkConsumer,
            Consumer<Throwable> throwableConsumer,
            boolean closeOnSummary,
            Supplier<Throwable> interruptSupplier,
            Logging logging) {
        this.boltConnection = boltConnection;
        this.legacyNotifications = new BoltProtocolVersion(5, 5).compareTo(boltConnection.protocolVersion()) > 0;
        this.query = query;
        if (runError == null) {
            this.runSummary = runSummary;
            this.state = State.READY;
        } else {
            this.runSummary = EMPTY_RUN_SUMMARY;
            this.state = State.FAILED;
            this.summaryFuture.completeExceptionally(runError);
        }
        this.runError = runError;
        this.bookmarkConsumer = bookmarkConsumer;
        this.closeOnSummary = closeOnSummary;
        this.throwableConsumer = throwableConsumer;
        this.interruptSupplier = interruptSupplier;
        this.log = logging.getLog(getClass());

        var runErrorName = runError == null ? "null" : runError.getClass().getCanonicalName();
        log.trace("[%d] New instance (runError=%s)", hashCode(), runErrorName);
    }

    @Override
    public synchronized Throwable getRunError() {
        var name = runError == null ? "null" : runError.getClass().getCanonicalName();
        log.trace("[%d] Run error explicitly retrieved (value=%s)", hashCode(), name);
        runErrorExposed = true;
        return runError;
    }

    @Override
    public List<String> keys() {
        return runSummary.keys();
    }

    @Override
    public CompletionStage<Void> consumed() {
        return consumedFuture;
    }

    @Override
    public synchronized boolean isDone() {
        return switch (state) {
            case DISCARDING, STREAMING, READY -> false;
            case FAILED -> runError == null || runErrorExposed;
            case SUCCEEDED -> true;
        };
    }

    @Override
    public void installRecordConsumer(BiConsumer<Record, Throwable> recordConsumer) {
        Objects.requireNonNull(recordConsumer);
        var runnable = NOOP_RUNNABLE;
        synchronized (this) {
            if (this.recordConsumer == null) {
                this.recordConsumer = (record, throwable) -> {
                    var recordHash = record == null ? "null" : record.hashCode();
                    var throwableName =
                            throwable == null ? "null" : throwable.getClass().getCanonicalName();
                    try {
                        recordConsumer.accept(record, throwable);
                        log.trace(
                                "[%d] Record consumer notified with (record=%s, throwable=%s)",
                                hashCode(), recordHash, throwableName);
                    } catch (Throwable unexpectedThrowable) {
                        log.error(
                                String.format(
                                        "[%d] Record consumer threw an error when notified with (record=%s, throwable=%s), this will be ignored",
                                        hashCode(), recordHash, throwableName),
                                unexpectedThrowable);
                    }
                };
                log.trace("[%d] Record consumer installed", hashCode());
                if (runError != null && !runErrorExposed) {
                    runnable = setupRecordConsumerErrorNotificationRunnable(runError, true);
                }
            } else {
                log.warn("[%d] Only one record consumer is supported, this request will be ignored", hashCode());
            }
        }
        runnable.run();
    }

    @Override
    public void request(long n) {
        if (n > 0) {
            var runnable = NOOP_RUNNABLE;
            synchronized (this) {
                if (recordConsumerFinished) {
                    log.trace(
                            "[%d] Tried requesting more records after record consumer is finished, this request will be ignored",
                            hashCode());
                    return;
                }
                recordConsumerHadRequests = true;
                updateRecordState(RecordState.NO_RECORD);
                log.trace("[%d] %d records requested in %s state", hashCode(), n, state);
                switch (state) {
                    case READY -> runnable = executeIfNotInterrupted(() -> {
                        var request = appendDemand(n);
                        state = State.STREAMING;
                        return () -> boltConnection
                                .pull(runSummary.queryId(), request)
                                .thenCompose(conn -> conn.flush(this))
                                .whenComplete((ignored, throwable) -> {
                                    throwable = Futures.completionExceptionCause(throwable);
                                    if (throwable != null) {
                                        handleError(throwable, false);
                                        onComplete();
                                    }
                                });
                    });
                    case STREAMING -> appendDemand(n);
                    case FAILED -> runnable = runError != null
                            ? setupRecordConsumerErrorNotificationRunnable(runError, true)
                            : error != null
                                    ? setupRecordConsumerErrorNotificationRunnable(error, false)
                                    : NOOP_RUNNABLE;
                    case DISCARDING, SUCCEEDED -> {}
                }
            }
            runnable.run();
        } else {
            log.warn("[%d] %d records requested, negative amounts will be ignored", hashCode(), n);
        }
    }

    @Override
    public void cancel() {
        var runnable = NOOP_RUNNABLE;
        synchronized (this) {
            log.trace("[%d] Cancellation requested in %s state", hashCode(), state);
            switch (state) {
                case READY -> runnable = executeIfNotInterrupted(this::setupDiscardRunnable);
                case STREAMING -> discardPending = true;
                case DISCARDING, FAILED, SUCCEEDED -> {}
            }
        }
        runnable.run();
    }

    @Override
    public CompletionStage<ResultSummary> summaryAsync() {
        var runnable = NOOP_RUNNABLE;
        synchronized (this) {
            log.trace("[%d] Summary requested in %s state", hashCode(), state);
            if (summaryExposed) {
                return summaryFuture;
            }
            summaryExposed = true;
            switch (state) {
                case SUCCEEDED, FAILED, DISCARDING -> {}
                case READY -> runnable = executeIfNotInterrupted(this::setupDiscardRunnable);
                case STREAMING -> discardPending = true;
            }
        }
        runnable.run();
        return summaryFuture;
    }

    @Override
    public CompletionStage<Void> rollback() {
        synchronized (this) {
            log.trace("[%d] Rolling back unpublished result %s state", hashCode(), state);
            switch (state) {
                case READY -> state = State.SUCCEEDED;
                case STREAMING, DISCARDING -> {
                    return summaryFuture.thenApply(ignored -> null);
                }
                case FAILED, SUCCEEDED -> {
                    return CompletableFuture.completedFuture(null);
                }
            }
        }
        var resetFuture = new CompletableFuture<Void>();
        boltConnection
                .reset()
                .thenCompose(conn -> conn.flush(new ResponseHandler() {
                    Throwable throwable = null;

                    @Override
                    public void onError(Throwable throwable) {
                        this.throwable = Futures.completionExceptionCause(throwable);
                    }

                    @Override
                    public void onComplete() {
                        if (throwable != null) {
                            resetFuture.completeExceptionally(throwable);
                        } else {
                            resetFuture.complete(null);
                        }
                    }
                }))
                .whenComplete((ignored, throwable) -> {
                    throwable = Futures.completionExceptionCause(throwable);
                    if (throwable != null) {
                        resetFuture.completeExceptionally(throwable);
                    }
                });
        return resetFuture
                .thenCompose(ignored -> boltConnection.close())
                .whenComplete((ignored, throwable) -> completeSummaryFuture(null, null))
                .exceptionally(throwable -> null);
    }

    @Override
    public void onComplete() {
        Runnable runnable;
        synchronized (this) {
            log.trace("[%d] onComplete", hashCode());
            var throwable = interruptSupplier.get();
            if (throwable != null) {
                handleError(throwable, true);
            } else {
                throwable = error;
            }

            if (throwable != null) {
                runnable = setupCompletionRunnableWithError(throwable);
            } else if (pullSummary != null) {
                runnable = setupCompletionRunnableWithPullSummary();
            } else if (discardSummary != null) {
                runnable = setupCompletionRunnableWithSummaryMetadata(discardSummary.metadata());
            } else {
                runnable = () -> log.trace("[%d] onComplete resulted in no action", hashCode());
            }
        }
        runnable.run();
    }

    @Override
    public synchronized void onError(Throwable throwable) {
        if (log.isTraceEnabled()) {
            log.error(String.format("[%d] onError", hashCode()), throwable);
        }
        handleError(throwable, false);
    }

    @Override
    public synchronized void onIgnored() {
        log.trace("[%d] onIgnored", hashCode());
        var throwable = interruptSupplier.get();
        if (throwable == null) {
            throwable = IGNORED_ERROR;
        }
        onError(throwable);
    }

    @Override
    public void onRecord(Value[] fields) {
        log.trace("[%d] onRecord", hashCode());
        synchronized (this) {
            updateRecordState(RecordState.HAD_RECORD);
            decrementDemand();
        }
        var record = new InternalRecord(runSummary.keys(), fields);
        recordConsumer.accept(record, null);
    }

    @Override
    public synchronized void onPullSummary(PullSummary summary) {
        log.trace("[%d] onPullSummary", hashCode());
        pullSummary = summary;
    }

    @Override
    public synchronized void onDiscardSummary(DiscardSummary summary) {
        log.trace("[%d] onDiscardSummary", hashCode());
        discardSummary = summary;
    }

    @Override
    public synchronized CompletionStage<Throwable> discardAllFailureAsync() {
        log.trace("[%d] Discard all requested", hashCode());
        var summaryExposed = this.summaryExposed;
        var runErrorExposed = this.runErrorExposed;
        return summaryAsync()
                .thenApply(ignored -> (Throwable) null)
                .exceptionally(throwable -> runErrorExposed || summaryExposed ? null : throwable);
    }

    @Override
    public synchronized CompletionStage<Throwable> pullAllFailureAsync() {
        log.trace("[%d] Pull all failure requested", hashCode());
        if (recordConsumer != null && !isDone()) {
            return CompletableFuture.completedFuture(
                    new TransactionNestingException(
                            "You cannot run another query or begin a new transaction in the same session before you've fully consumed the previous run result."));
        }
        return discardAllFailureAsync();
    }

    private synchronized long appendDemand(long n) {
        if (n == Long.MAX_VALUE) {
            outstandingDemand = -1;
        } else {
            try {
                outstandingDemand = Math.addExact(outstandingDemand, n);
            } catch (ArithmeticException ex) {
                outstandingDemand = -1;
            }
        }
        log.trace("[%d] Appended demand, outstanding is %d", hashCode(), outstandingDemand);
        return outstandingDemand;
    }

    private synchronized long getDemand() {
        log.trace("[%d] Get demand, outstanding is %d", hashCode(), outstandingDemand);
        return outstandingDemand;
    }

    private synchronized void decrementDemand() {
        if (outstandingDemand > 0) {
            outstandingDemand--;
        }
        log.trace("[%d] Decremented demand, outstanding is %d", hashCode(), outstandingDemand);
    }

    private synchronized Runnable setupDiscardRunnable() {
        state = State.DISCARDING;
        return () -> boltConnection
                .discard(runSummary.queryId(), -1)
                .thenCompose(conn -> conn.flush(this))
                .whenComplete((ignored, throwable) -> {
                    throwable = Futures.completionExceptionCause(throwable);
                    if (throwable != null) {
                        handleError(throwable, false);
                        onComplete();
                    }
                });
    }

    private synchronized Runnable executeIfNotInterrupted(Supplier<Runnable> runnableSupplier) {
        var runnable = NOOP_RUNNABLE;
        var throwable = interruptSupplier.get();
        if (throwable == null) {
            runnable = runnableSupplier.get();
        } else {
            log.trace("[%d] Interrupt signal detected upon handling request", hashCode());
            handleError(throwable, true);
            runnable = this::onComplete;
        }
        return runnable;
    }

    private synchronized Runnable setupRecordConsumerErrorNotificationRunnable(Throwable throwable, boolean runError) {
        Runnable runnable;
        if (recordConsumer != null) {
            if (!recordConsumerFinished) {
                if (runError) {
                    this.runErrorExposed = true;
                }
                recordConsumerFinished = true;
                var recordConsumerRef = recordConsumer;
                recordConsumer = NOOP_CONSUMER;
                runnable = () -> recordConsumerRef.accept(null, throwable);
            } else {
                runnable = () ->
                        log.trace("[%d] Record consumer will not be notified as it has been finished", hashCode());
            }
        } else {
            runnable = () ->
                    log.trace("[%d] Record consumer will not be notified as it has not been installed", hashCode());
        }
        return runnable;
    }

    private synchronized Runnable setupCompletionRunnableWithPullSummary() {
        log.trace("[%d] Setting up completion with pull summary", hashCode());
        var runnable = NOOP_RUNNABLE;
        if (pullSummary.hasMore()) {
            pullSummary = null;
            if (discardPending) {
                discardPending = false;
                state = State.DISCARDING;
                runnable = () -> boltConnection
                        .discard(runSummary.queryId(), -1)
                        .thenCompose(conn -> conn.flush(this))
                        .whenComplete((ignored, flushThrowable) -> {
                            var error = Futures.completionExceptionCause(flushThrowable);
                            if (error != null) {
                                handleError(error, false);
                                onComplete();
                            }
                        });
            } else {
                var demand = getDemand();
                if (demand != 0) {
                    state = State.STREAMING;
                    runnable = () -> boltConnection
                            .pull(runSummary.queryId(), demand > 0 ? demand : -1)
                            .thenCompose(conn -> conn.flush(this))
                            .whenComplete((ignored, flushThrowable) -> {
                                var error = Futures.completionExceptionCause(flushThrowable);
                                if (error != null) {
                                    handleError(error, false);
                                    onComplete();
                                }
                            });
                } else {
                    state = State.READY;
                }
            }
        } else {
            runnable = setupCompletionRunnableWithSummaryMetadata(pullSummary.metadata());
        }
        return runnable;
    }

    private synchronized Runnable setupCompletionRunnableWithSummaryMetadata(Map<String, Value> metadata) {
        log.trace("[%d] Setting up completion with summary metadata", hashCode());
        var runnable = NOOP_RUNNABLE;
        ResultSummary resultSummary = null;
        try {
            resultSummary = resultSummary(metadata);
            state = State.SUCCEEDED;
        } catch (Throwable summaryThrowable) {
            handleError(summaryThrowable, false);
        }

        if (resultSummary != null) {
            var bookmarkOpt = databaseBookmark(metadata);
            var recordConsumerFinished = this.recordConsumerFinished;
            this.recordConsumerFinished = true;
            var recordConsumerRef = recordConsumer;
            this.recordConsumer = NOOP_CONSUMER;
            var recordConsumerHadRequests = this.recordConsumerHadRequests;
            var resultSummaryRef = resultSummary;

            runnable = () -> {
                bookmarkOpt.ifPresent(bookmarkConsumer);
                var closeStage = closeOnSummary ? boltConnection.close() : CompletableFuture.completedStage(null);
                closeStage.whenComplete((ignored, closeThrowable) -> {
                    var error = Futures.completionExceptionCause(closeThrowable);
                    if (error != null) {
                        if (log.isTraceEnabled()) {
                            log.error(
                                    String.format(
                                            "[%d] Failed to close connection while publishing summary", hashCode()),
                                    error);
                        }
                    }
                    if (recordConsumerFinished) {
                        log.trace("[%d] Won't publish summary because recordConsumer is finished", hashCode());
                    } else {
                        if (recordConsumerRef != null) {
                            if (recordConsumerHadRequests) {
                                recordConsumerRef.accept(null, null);
                            } else {
                                log.trace(
                                        "[%d] Record consumer will not be notified as it had no requests", hashCode());
                            }
                        } else {
                            log.trace(
                                    "[%d] Record consumer will not be notified as it has not been installed",
                                    hashCode());
                        }
                    }
                    completeSummaryFuture(resultSummaryRef, null);
                });
            };
        } else {
            runnable = this::onComplete;
        }
        return runnable;
    }

    private ResultSummary resultSummary(Map<String, Value> metadata) {
        return METADATA_EXTRACTOR.extractSummary(
                query,
                boltConnection,
                runSummary.resultAvailableAfter(),
                metadata,
                legacyNotifications,
                generateGqlStatusObject(runSummary.keys()));
    }

    @SuppressWarnings("DuplicatedCode")
    private static Optional<DatabaseBookmark> databaseBookmark(Map<String, Value> metadata) {
        DatabaseBookmark databaseBookmark = null;
        var bookmarkValue = metadata.get("bookmark");
        if (bookmarkValue != null && !bookmarkValue.isNull() && bookmarkValue.hasType(TYPE_SYSTEM.STRING())) {
            var bookmarkStr = bookmarkValue.asString();
            if (!bookmarkStr.isEmpty()) {
                databaseBookmark = new DatabaseBookmark(null, Bookmark.from(bookmarkStr));
            }
        }
        return Optional.ofNullable(databaseBookmark);
    }

    private synchronized Runnable setupCompletionRunnableWithError(Throwable throwable) {
        log.trace(
                "[%d] Setting up completion with error %s",
                hashCode(), throwable.getClass().getCanonicalName());
        var recordConsumerPresent = this.recordConsumer != null;
        var recordConsumerFinished = this.recordConsumerFinished;
        var recordConsumerErrorNotificationRunnable = setupRecordConsumerErrorNotificationRunnable(throwable, false);
        var interrupted = this.interrupted;
        return () -> {
            ResultSummary summary = null;
            try {
                summary = resultSummary(Collections.emptyMap());
            } catch (Throwable summaryThrowable) {
                if (!interrupted) {
                    throwable.addSuppressed(summaryThrowable);
                }
            }

            if (summary != null && recordConsumerPresent && !recordConsumerFinished) {
                var summaryRef = summary;
                closeBoltConnection(throwable, interrupted, () -> {
                    // notify recordConsumer when possible
                    recordConsumerErrorNotificationRunnable.run();
                    completeSummaryFuture(summaryRef, null);
                });
            } else {
                closeBoltConnection(throwable, interrupted, () -> completeSummaryFuture(null, throwable));
            }
        };
    }

    private void closeBoltConnection(Throwable throwable, boolean interrupted, Runnable runnable) {
        var closeStage = closeOnSummary ? boltConnection.close() : CompletableFuture.completedStage(null);
        closeStage.whenComplete((ignored, closeThrowable) -> {
            var error = Futures.completionExceptionCause(closeThrowable);
            if (!interrupted) {
                if (error != null) {
                    throwable.addSuppressed(error);
                }
                throwableConsumer.accept(throwable);
            }
            runnable.run();
        });
    }

    private synchronized void handleError(Throwable throwable, boolean interrupted) {
        state = State.FAILED;
        throwable = Futures.completionExceptionCause(throwable);
        if (error == null) {
            error = throwable;
            this.interrupted = interrupted;
        } else {
            if (!this.interrupted) {
                if (throwable == IGNORED_ERROR) {
                    return;
                }
                if (interrupted) {
                    error = throwable;
                    this.interrupted = true;
                } else {
                    if (error instanceof Neo4jException && !(throwable instanceof Neo4jException)) {
                        // higher order error has occurred
                        if (error != IGNORED_ERROR) {
                            throwable.addSuppressed(error);
                        }
                        error = throwable;
                    } else {
                        error.addSuppressed(throwable);
                    }
                }
            }
        }
    }

    private void completeSummaryFuture(ResultSummary summary, Throwable throwable) {
        throwable = Futures.completionExceptionCause(throwable);
        if (throwable != null) {
            consumedFuture.completeExceptionally(throwable);
            summaryFuture.completeExceptionally(throwable);
        } else {
            consumedFuture.complete(null);
            summaryFuture.complete(summary);
        }
    }
}
