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
package org.neo4j.driver.internal;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.AuthData;
import org.neo4j.driver.internal.bolt.api.BoltAgent;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltConnectionProvider;
import org.neo4j.driver.internal.bolt.api.BoltConnectionState;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.GqlStatusError;
import org.neo4j.driver.internal.bolt.api.MetricsListener;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.ResponseHandler;
import org.neo4j.driver.internal.bolt.api.RoutingContext;
import org.neo4j.driver.internal.bolt.api.SecurityPlan;
import org.neo4j.driver.internal.bolt.api.TelemetryApi;
import org.neo4j.driver.internal.bolt.api.TransactionType;
import org.neo4j.driver.internal.bolt.api.exception.BoltException;
import org.neo4j.driver.internal.bolt.api.exception.BoltFailureException;
import org.neo4j.driver.internal.bolt.api.exception.ConnectionReadTimeoutException;
import org.neo4j.driver.internal.bolt.api.exception.DiscoveryException;
import org.neo4j.driver.internal.bolt.api.exception.MinVersionAcquisitionException;
import org.neo4j.driver.internal.bolt.api.exception.ProtocolException;
import org.neo4j.driver.internal.bolt.api.exception.ServiceUnavailableException;
import org.neo4j.driver.internal.bolt.api.exception.SessionExpiredException;
import org.neo4j.driver.internal.bolt.api.exception.UnsupportedFeatureException;
import org.neo4j.driver.internal.bolt.api.exception.UntrustedServerException;
import org.neo4j.driver.internal.bolt.api.summary.BeginSummary;
import org.neo4j.driver.internal.bolt.api.summary.CommitSummary;
import org.neo4j.driver.internal.bolt.api.summary.DiscardSummary;
import org.neo4j.driver.internal.bolt.api.summary.LogoffSummary;
import org.neo4j.driver.internal.bolt.api.summary.LogonSummary;
import org.neo4j.driver.internal.bolt.api.summary.PullSummary;
import org.neo4j.driver.internal.bolt.api.summary.ResetSummary;
import org.neo4j.driver.internal.bolt.api.summary.RollbackSummary;
import org.neo4j.driver.internal.bolt.api.summary.RouteSummary;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;
import org.neo4j.driver.internal.bolt.api.summary.TelemetrySummary;
import org.neo4j.driver.internal.util.ErrorUtil;

public class ErrorMappingBoltConnectionProvider implements BoltConnectionProvider {
    private final BoltConnectionProvider delegate;

    public ErrorMappingBoltConnectionProvider(BoltConnectionProvider delegate) {
        this.delegate = delegate;
    }

    @Override
    public CompletionStage<Void> init(
            BoltServerAddress address,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            MetricsListener metricsListener) {
        return map(delegate.init(address, routingContext, boltAgent, userAgent, connectTimeoutMillis, metricsListener));
    }

    @Override
    public CompletionStage<BoltConnection> connect(
            SecurityPlan securityPlan,
            DatabaseName databaseName,
            Supplier<CompletionStage<Map<String, Value>>> authMapStageSupplier,
            AccessMode mode,
            Set<String> bookmarks,
            String impersonatedUser,
            BoltProtocolVersion minVersion,
            NotificationConfig notificationConfig,
            Consumer<DatabaseName> databaseNameConsumer) {
        return map(delegate.connect(
                        securityPlan,
                        databaseName,
                        authMapStageSupplier,
                        mode,
                        bookmarks,
                        impersonatedUser,
                        minVersion,
                        notificationConfig,
                        databaseNameConsumer))
                .thenApply(ErrorMappingBoltConnection::new);
    }

    @Override
    public CompletionStage<Void> verifyConnectivity(SecurityPlan securityPlan, Map<String, Value> authMap) {
        return map(delegate.verifyConnectivity(securityPlan, authMap));
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDb(SecurityPlan securityPlan, Map<String, Value> authMap) {
        return map(delegate.supportsMultiDb(securityPlan, authMap));
    }

    @Override
    public CompletionStage<Boolean> supportsSessionAuth(SecurityPlan securityPlan, Map<String, Value> authMap) {
        return map(delegate.supportsSessionAuth(securityPlan, authMap));
    }

    @Override
    public CompletionStage<Void> close() {
        return map(delegate.close());
    }

    private static <T> CompletionStage<T> map(CompletionStage<T> stage) {
        return stage.exceptionally(throwable -> {
            throw new CompletionException(map(throwable));
        });
    }

    private static Throwable map(Throwable throwable) {
        if (throwable instanceof CompletionException) {
            throwable = throwable.getCause();
        }

        if (throwable instanceof BoltFailureException exception) {
            return ErrorUtil.map(exception);
        } else {
            if (throwable instanceof ConnectionReadTimeoutException) {
                return org.neo4j.driver.exceptions.ConnectionReadTimeoutException.INSTANCE;
            } else if (throwable instanceof ServiceUnavailableException) {
                return new org.neo4j.driver.exceptions.ServiceUnavailableException(throwable.getMessage(), throwable);
            } else if (throwable instanceof UnsupportedFeatureException) {
                return new org.neo4j.driver.exceptions.UnsupportedFeatureException(throwable.getMessage(), throwable);
            } else if (throwable instanceof SessionExpiredException) {
                return new org.neo4j.driver.exceptions.SessionExpiredException(throwable.getMessage(), throwable);
            } else if (throwable instanceof DiscoveryException) {
                return new org.neo4j.driver.exceptions.DiscoveryException(throwable.getMessage(), throwable);
            } else if (throwable instanceof UntrustedServerException) {
                return new org.neo4j.driver.exceptions.UntrustedServerException(throwable.getMessage());
            } else if (throwable instanceof ProtocolException) {
                return new org.neo4j.driver.exceptions.ProtocolException(throwable.getMessage(), throwable);
            } else if (throwable instanceof BoltException && !(throwable instanceof MinVersionAcquisitionException)) {
                return new Neo4jException(
                        GqlStatusError.UNKNOWN.getStatus(),
                        GqlStatusError.UNKNOWN.getStatusDescription(throwable.getMessage()),
                        "N/A",
                        throwable.getMessage(),
                        GqlStatusError.DIAGNOSTIC_RECORD,
                        throwable);
            } else {
                return throwable;
            }
        }
    }

    private static class ErrorMappingBoltConnection implements BoltConnection {
        private final BoltConnection delegate;

        private ErrorMappingBoltConnection(BoltConnection delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletionStage<BoltConnection> route(
                DatabaseName databaseName, String impersonatedUser, Set<String> bookmarks) {
            return map(delegate.route(databaseName, impersonatedUser, bookmarks));
        }

        @Override
        public CompletionStage<BoltConnection> beginTransaction(
                DatabaseName databaseName,
                AccessMode accessMode,
                String impersonatedUser,
                Set<String> bookmarks,
                TransactionType transactionType,
                Duration txTimeout,
                Map<String, Value> txMetadata,
                String txType,
                NotificationConfig notificationConfig) {
            return map(delegate.beginTransaction(
                    databaseName,
                    accessMode,
                    impersonatedUser,
                    bookmarks,
                    transactionType,
                    txTimeout,
                    txMetadata,
                    txType,
                    notificationConfig));
        }

        @Override
        public CompletionStage<BoltConnection> runInAutoCommitTransaction(
                DatabaseName databaseName,
                AccessMode accessMode,
                String impersonatedUser,
                Set<String> bookmarks,
                String query,
                Map<String, Value> parameters,
                Duration txTimeout,
                Map<String, Value> txMetadata,
                NotificationConfig notificationConfig) {
            return map(delegate.runInAutoCommitTransaction(
                    databaseName,
                    accessMode,
                    impersonatedUser,
                    bookmarks,
                    query,
                    parameters,
                    txTimeout,
                    txMetadata,
                    notificationConfig));
        }

        @Override
        public CompletionStage<BoltConnection> run(String query, Map<String, Value> parameters) {
            return map(delegate.run(query, parameters));
        }

        @Override
        public CompletionStage<BoltConnection> pull(long qid, long request) {
            return map(delegate.pull(qid, request));
        }

        @Override
        public CompletionStage<BoltConnection> discard(long qid, long number) {
            return map(delegate.discard(qid, number));
        }

        @Override
        public CompletionStage<BoltConnection> commit() {
            return map(delegate.commit());
        }

        @Override
        public CompletionStage<BoltConnection> rollback() {
            return map(delegate.rollback());
        }

        @Override
        public CompletionStage<BoltConnection> reset() {
            return map(delegate.reset());
        }

        @Override
        public CompletionStage<BoltConnection> logoff() {
            return map(delegate.logoff());
        }

        @Override
        public CompletionStage<BoltConnection> logon(Map<String, Value> authMap) {
            return map(delegate.logon(authMap));
        }

        @Override
        public CompletionStage<BoltConnection> telemetry(TelemetryApi telemetryApi) {
            return map(delegate.telemetry(telemetryApi));
        }

        @Override
        public CompletionStage<BoltConnection> clear() {
            return map(delegate.clear());
        }

        @Override
        public CompletionStage<Void> flush(ResponseHandler handler) {
            return map(delegate.flush(new ResponseHandler() {
                public void onError(Throwable throwable) {
                    handler.onError(map(throwable));
                }

                public void onBeginSummary(BeginSummary summary) {
                    handler.onBeginSummary(summary);
                }

                public void onRunSummary(RunSummary summary) {
                    handler.onRunSummary(summary);
                }

                public void onRecord(Value[] fields) {
                    handler.onRecord(fields);
                }

                public void onPullSummary(PullSummary summary) {
                    handler.onPullSummary(summary);
                }

                public void onDiscardSummary(DiscardSummary summary) {
                    handler.onDiscardSummary(summary);
                }

                public void onCommitSummary(CommitSummary summary) {
                    handler.onCommitSummary(summary);
                }

                public void onRollbackSummary(RollbackSummary summary) {
                    handler.onRollbackSummary(summary);
                }

                public void onResetSummary(ResetSummary summary) {
                    handler.onResetSummary(summary);
                }

                public void onRouteSummary(RouteSummary summary) {
                    handler.onRouteSummary(summary);
                }

                public void onLogoffSummary(LogoffSummary summary) {
                    handler.onLogoffSummary(summary);
                }

                public void onLogonSummary(LogonSummary summary) {
                    handler.onLogonSummary(summary);
                }

                public void onTelemetrySummary(TelemetrySummary summary) {
                    handler.onTelemetrySummary(summary);
                }

                public void onIgnored() {
                    handler.onIgnored();
                }

                public void onComplete() {
                    handler.onComplete();
                }
            }));
        }

        @Override
        public CompletionStage<Void> forceClose(String reason) {
            return map(delegate.forceClose(reason));
        }

        @Override
        public CompletionStage<Void> close() {
            return map(delegate.close());
        }

        @Override
        public BoltConnectionState state() {
            return delegate.state();
        }

        @Override
        public CompletionStage<AuthData> authData() {
            return map(delegate.authData());
        }

        @Override
        public String serverAgent() {
            return delegate.serverAgent();
        }

        @Override
        public BoltServerAddress serverAddress() {
            return delegate.serverAddress();
        }

        @Override
        public BoltProtocolVersion protocolVersion() {
            return delegate.protocolVersion();
        }

        @Override
        public boolean telemetrySupported() {
            return delegate.telemetrySupported();
        }
    }
}
