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
package org.neo4j.driver.internal.async;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.ResponseHandler;
import org.neo4j.driver.internal.util.Futures;

final class TerminationAwareBoltConnection extends DelegatingBoltConnection {
    private final Logging logging;
    private final Logger log;
    private final TerminationAwareStateLockingExecutor executor;
    private final Consumer<Throwable> throwableConsumer;

    public TerminationAwareBoltConnection(
            Logging logging,
            BoltConnection delegate,
            TerminationAwareStateLockingExecutor executor,
            Consumer<Throwable> throwableConsumer) {
        super(delegate);
        this.logging = Objects.requireNonNull(logging);
        this.log = logging.getLog(getClass());
        this.executor = Objects.requireNonNull(executor);
        this.throwableConsumer = Objects.requireNonNull(throwableConsumer);
    }

    public CompletionStage<BoltConnection> clearAndReset() {
        var future = new CompletableFuture<BoltConnection>();
        var thisVal = this;

        delegate.onLoop()
                .thenCompose(connection -> executor.execute(ignored -> connection
                        .clear()
                        .thenCompose(BoltConnection::reset)
                        .thenCompose(conn -> conn.flush(new ResponseHandler() {
                            Throwable throwable = null;

                            @Override
                            public void onError(Throwable throwable) {
                                log.error("Unexpected error occurred while resetting connection", throwable);
                                throwableConsumer.accept(throwable);
                                this.throwable = throwable;
                            }

                            @Override
                            public void onComplete() {
                                if (throwable != null) {
                                    future.completeExceptionally(throwable);
                                } else {
                                    future.complete(thisVal);
                                }
                            }
                        }))))
                .whenComplete((ignored, throwable) -> {
                    if (throwable != null) {
                        throwableConsumer.accept(throwable);
                        future.completeExceptionally(throwable);
                    }
                });

        return future;
    }

    @Override
    public CompletionStage<Void> flush(ResponseHandler handler) {
        return delegate.onLoop()
                .thenCompose(connection -> executor.execute(causeOfTermination -> {
                    if (causeOfTermination == null) {
                        log.trace("This connection is active, will flush");
                        var terminationAwareResponseHandler =
                                new TerminationAwareResponseHandler(logging, handler, executor, throwableConsumer);
                        return delegate.flush(terminationAwareResponseHandler).handle((ignored, flushThrowable) -> {
                            flushThrowable = Futures.completionExceptionCause(flushThrowable);
                            if (flushThrowable != null) {
                                if (log.isTraceEnabled()) {
                                    log.error("The flush has failed", flushThrowable);
                                }
                                var flushThrowableRef = flushThrowable;
                                flushThrowable = executor.execute(existingThrowable -> {
                                    if (existingThrowable != null) {
                                        log.trace(
                                                "The flush has failed, but there is an existing %s", existingThrowable);
                                        return existingThrowable;
                                    } else {
                                        throwableConsumer.accept(flushThrowableRef);
                                        return flushThrowableRef;
                                    }
                                });
                                // rethrow
                                if (flushThrowable instanceof RuntimeException runtimeException) {
                                    throw runtimeException;
                                } else {
                                    throw new CompletionException(flushThrowable);
                                }
                            } else {
                                return ignored;
                            }
                        });
                    } else {
                        // there is an existing error
                        return connection
                                .clear()
                                .thenCompose(ignored -> CompletableFuture.failedStage(causeOfTermination));
                    }
                }));
    }
}
