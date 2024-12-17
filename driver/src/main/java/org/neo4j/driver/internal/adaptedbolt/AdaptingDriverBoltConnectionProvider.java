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

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltAgent;
import org.neo4j.driver.internal.bolt.api.BoltConnectionProvider;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.MetricsListener;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.RoutingContext;
import org.neo4j.driver.internal.bolt.api.SecurityPlan;
import org.neo4j.driver.internal.value.BoltValueFactory;

public class AdaptingDriverBoltConnectionProvider implements DriverBoltConnectionProvider {
    private final BoltConnectionProvider delegate;
    private final ErrorMapper errorMapper;
    private final BoltValueFactory boltValueFactory;
    private final boolean routed;

    public AdaptingDriverBoltConnectionProvider(
            BoltConnectionProvider delegate,
            ErrorMapper errorMapper,
            BoltValueFactory boltValueFactory,
            boolean routed) {
        this.delegate = Objects.requireNonNull(delegate);
        this.errorMapper = Objects.requireNonNull(errorMapper);
        this.boltValueFactory = Objects.requireNonNull(boltValueFactory);
        this.routed = routed;
    }

    @Override
    public CompletionStage<Void> init(
            BoltServerAddress address,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            MetricsListener metricsListener) {
        return delegate.init(address, routingContext, boltAgent, userAgent, connectTimeoutMillis, metricsListener)
                .exceptionally(errorMapper::mapAndTrow);
    }

    @Override
    public CompletionStage<DriverBoltConnection> connect(
            SecurityPlan securityPlan,
            DatabaseName databaseName,
            Supplier<CompletionStage<Map<String, Value>>> authMapStageSupplier,
            AccessMode mode,
            Set<String> bookmarks,
            String impersonatedUser,
            BoltProtocolVersion minVersion,
            NotificationConfig notificationConfig,
            Consumer<DatabaseName> databaseNameConsumer) {
        return delegate.connect(
                        securityPlan,
                        databaseName,
                        () -> authMapStageSupplier.get().thenApply(boltValueFactory::toBoltMap),
                        mode,
                        bookmarks,
                        impersonatedUser,
                        minVersion,
                        notificationConfig,
                        databaseNameConsumer)
                .exceptionally(errorMapper::mapAndTrow)
                .thenApply(boltConnection -> new AdaptingDriverBoltConnection(
                        boltConnection,
                        routed ? new RoutedErrorMapper(boltConnection.serverAddress(), mode) : errorMapper,
                        boltValueFactory));
    }

    @Override
    public CompletionStage<Void> verifyConnectivity(SecurityPlan securityPlan, Map<String, Value> authMap) {
        return delegate.verifyConnectivity(securityPlan, boltValueFactory.toBoltMap(authMap))
                .exceptionally(errorMapper::mapAndTrow);
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDb(SecurityPlan securityPlan, Map<String, Value> authMap) {
        return delegate.supportsMultiDb(securityPlan, boltValueFactory.toBoltMap(authMap))
                .exceptionally(errorMapper::mapAndTrow);
    }

    @Override
    public CompletionStage<Boolean> supportsSessionAuth(SecurityPlan securityPlan, Map<String, Value> authMap) {
        return delegate.supportsSessionAuth(securityPlan, boltValueFactory.toBoltMap(authMap))
                .exceptionally(errorMapper::mapAndTrow);
    }

    @Override
    public CompletionStage<Void> close() {
        return delegate.close().exceptionally(errorMapper::mapAndTrow);
    }
}
