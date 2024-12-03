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
package org.neo4j.driver.internal.adaptedbolt;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.AuthData;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltConnectionState;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.TelemetryApi;
import org.neo4j.driver.internal.bolt.api.TransactionType;

final class AdaptingDriverBoltConnection implements DriverBoltConnection {
    private final BoltConnection connection;
    private final ErrorMapper errorMapper;

    AdaptingDriverBoltConnection(BoltConnection connection, ErrorMapper errorMapper) {
        this.connection = Objects.requireNonNull(connection);
        this.errorMapper = Objects.requireNonNull(errorMapper);
    }

    @Override
    public CompletionStage<DriverBoltConnection> onLoop() {
        return connection.onLoop().exceptionally(errorMapper::mapAndTrow).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<DriverBoltConnection> route(
            DatabaseName databaseName, String impersonatedUser, Set<String> bookmarks) {
        return connection
                .route(databaseName, impersonatedUser, bookmarks)
                .exceptionally(errorMapper::mapAndTrow)
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<DriverBoltConnection> beginTransaction(
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            Set<String> bookmarks,
            TransactionType transactionType,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            String txType,
            NotificationConfig notificationConfig) {
        return connection
                .beginTransaction(
                        databaseName,
                        accessMode,
                        impersonatedUser,
                        bookmarks,
                        transactionType,
                        txTimeout,
                        txMetadata,
                        txType,
                        notificationConfig)
                .exceptionally(errorMapper::mapAndTrow)
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<DriverBoltConnection> runInAutoCommitTransaction(
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            Set<String> bookmarks,
            String query,
            Map<String, Value> parameters,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            NotificationConfig notificationConfig) {
        return connection
                .runInAutoCommitTransaction(
                        databaseName,
                        accessMode,
                        impersonatedUser,
                        bookmarks,
                        query,
                        parameters,
                        txTimeout,
                        txMetadata,
                        notificationConfig)
                .exceptionally(errorMapper::mapAndTrow)
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<DriverBoltConnection> run(String query, Map<String, Value> parameters) {
        return connection
                .run(query, parameters)
                .exceptionally(errorMapper::mapAndTrow)
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<DriverBoltConnection> pull(long qid, long request) {
        return connection
                .pull(qid, request)
                .exceptionally(errorMapper::mapAndTrow)
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<DriverBoltConnection> discard(long qid, long number) {
        return connection
                .discard(qid, number)
                .exceptionally(errorMapper::mapAndTrow)
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<DriverBoltConnection> commit() {
        return connection.commit().exceptionally(errorMapper::mapAndTrow).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<DriverBoltConnection> rollback() {
        return connection.rollback().exceptionally(errorMapper::mapAndTrow).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<DriverBoltConnection> reset() {
        return connection.reset().exceptionally(errorMapper::mapAndTrow).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<DriverBoltConnection> logoff() {
        return connection.logoff().exceptionally(errorMapper::mapAndTrow).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<DriverBoltConnection> logon(Map<String, Value> authMap) {
        return connection.logon(authMap).exceptionally(errorMapper::mapAndTrow).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<DriverBoltConnection> telemetry(TelemetryApi telemetryApi) {
        return connection
                .telemetry(telemetryApi)
                .exceptionally(errorMapper::mapAndTrow)
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<DriverBoltConnection> clear() {
        return connection.clear().exceptionally(errorMapper::mapAndTrow).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<Void> flush(DriverResponseHandler handler) {
        return connection
                .flush(new AdaptingDriverResponseHandler(handler, errorMapper))
                .exceptionally(errorMapper::mapAndTrow);
    }

    @Override
    public CompletionStage<Void> forceClose(String reason) {
        return connection.forceClose(reason).exceptionally(errorMapper::mapAndTrow);
    }

    @Override
    public CompletionStage<Void> close() {
        return connection.close().exceptionally(errorMapper::mapAndTrow);
    }

    @Override
    public BoltConnectionState state() {
        return connection.state();
    }

    @Override
    public CompletionStage<AuthData> authData() {
        return connection.authData().exceptionally(errorMapper::mapAndTrow);
    }

    @Override
    public String serverAgent() {
        return connection.serverAgent();
    }

    @Override
    public BoltServerAddress serverAddress() {
        return connection.serverAddress();
    }

    @Override
    public BoltProtocolVersion protocolVersion() {
        return connection.protocolVersion();
    }

    @Override
    public boolean telemetrySupported() {
        return connection.telemetrySupported();
    }
}
