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
package org.neo4j.driver.internal.boltlistener;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.AuthData;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltConnectionState;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.ResponseHandler;
import org.neo4j.driver.internal.bolt.api.TelemetryApi;
import org.neo4j.driver.internal.bolt.api.TransactionType;
import org.neo4j.driver.internal.bolt.api.values.Value;

final class ListeningBoltConnection implements BoltConnection {
    private final BoltConnection delegate;
    private final BoltConnectionListener boltConnectionListener;

    public ListeningBoltConnection(BoltConnection delegate, BoltConnectionListener boltConnectionListener) {
        this.delegate = Objects.requireNonNull(delegate);
        this.boltConnectionListener = Objects.requireNonNull(boltConnectionListener);
    }

    @Override
    public CompletionStage<BoltConnection> onLoop() {
        return delegate.onLoop().thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> route(
            DatabaseName databaseName, String impersonatedUser, Set<String> bookmarks) {
        return delegate.route(databaseName, impersonatedUser, bookmarks).thenApply(ignored -> this);
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
        return delegate.beginTransaction(
                        databaseName,
                        accessMode,
                        impersonatedUser,
                        bookmarks,
                        transactionType,
                        txTimeout,
                        txMetadata,
                        txType,
                        notificationConfig)
                .thenApply(ignored -> this);
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
        return delegate.runInAutoCommitTransaction(
                        databaseName,
                        accessMode,
                        impersonatedUser,
                        bookmarks,
                        query,
                        parameters,
                        txTimeout,
                        txMetadata,
                        notificationConfig)
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> run(String query, Map<String, Value> parameters) {
        return delegate.run(query, parameters).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> pull(long qid, long request) {
        return delegate.pull(qid, request).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> discard(long qid, long number) {
        return delegate.discard(qid, number).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> commit() {
        return delegate.commit().thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> rollback() {
        return delegate.rollback().thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> reset() {
        return delegate.reset().thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> logoff() {
        return delegate.logoff().thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> logon(Map<String, Value> authMap) {
        return delegate.logon(authMap).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> telemetry(TelemetryApi telemetryApi) {
        return delegate.telemetry(telemetryApi).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> clear() {
        return delegate.clear().thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<Void> flush(ResponseHandler handler) {
        return delegate.flush(handler);
    }

    @Override
    public CompletionStage<Void> forceClose(String reason) {
        return delegate.forceClose(reason).whenComplete((ignored, throwable) -> boltConnectionListener.onClose(this));
    }

    @Override
    public CompletionStage<Void> close() {
        return delegate.close().whenComplete((ignored, throwable) -> boltConnectionListener.onClose(this));
    }

    @Override
    public BoltConnectionState state() {
        return delegate.state();
    }

    @Override
    public CompletionStage<AuthData> authData() {
        return delegate.authData();
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

    @Override
    public boolean serverSideRoutingEnabled() {
        return delegate.serverSideRoutingEnabled();
    }
}
