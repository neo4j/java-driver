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
package org.neo4j.driver.internal.bolt.basicimpl;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.util.internal.logging.InternalLoggerFactory;
import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.AuthToken;
import org.neo4j.driver.internal.bolt.api.BoltAgent;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltConnectionProvider;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.DomainNameResolver;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.MetricsListener;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.RoutingContext;
import org.neo4j.driver.internal.bolt.api.SecurityPlan;
import org.neo4j.driver.internal.bolt.api.exception.MinVersionAcquisitionException;
import org.neo4j.driver.internal.bolt.api.values.ValueFactory;
import org.neo4j.driver.internal.bolt.basicimpl.impl.BoltConnectionImpl;
import org.neo4j.driver.internal.bolt.basicimpl.impl.ConnectionProvider;
import org.neo4j.driver.internal.bolt.basicimpl.impl.ConnectionProviders;
import org.neo4j.driver.internal.bolt.basicimpl.impl.NettyLogging;
import org.neo4j.driver.internal.bolt.basicimpl.impl.NoopMetricsListener;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v51.BoltProtocolV51;
import org.neo4j.driver.internal.bolt.basicimpl.impl.util.FutureUtil;

public final class NettyBoltConnectionProvider implements BoltConnectionProvider {
    private final LoggingProvider logging;
    private final System.Logger log;
    private final ConnectionProvider connectionProvider;
    private final MetricsListener metricsListener;
    private final Clock clock;
    private final ValueFactory valueFactory;

    private CompletableFuture<Void> closeFuture;

    public NettyBoltConnectionProvider(
            EventLoopGroup eventLoopGroup,
            Clock clock,
            DomainNameResolver domainNameResolver,
            LocalAddress localAddress,
            LoggingProvider logging,
            ValueFactory valueFactory,
            MetricsListener metricsListener) {
        Objects.requireNonNull(eventLoopGroup);
        this.clock = Objects.requireNonNull(clock);
        this.logging = Objects.requireNonNull(logging);
        this.log = logging.getLog(getClass());
        this.connectionProvider = ConnectionProviders.netty(
                eventLoopGroup, clock, domainNameResolver, localAddress, logging, valueFactory);
        this.valueFactory = Objects.requireNonNull(valueFactory);
        this.metricsListener = NoopMetricsListener.getInstance();
        InternalLoggerFactory.setDefaultFactory(new NettyLogging(logging));
    }

    @Override
    public CompletionStage<BoltConnection> connect(
            BoltServerAddress address,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            SecurityPlan securityPlan,
            DatabaseName databaseName,
            Supplier<CompletionStage<AuthToken>> authTokenStageSupplier,
            AccessMode mode,
            Set<String> bookmarks,
            String impersonatedUser,
            BoltProtocolVersion minVersion,
            NotificationConfig notificationConfig,
            Consumer<DatabaseName> databaseNameConsumer,
            Map<String, Object> additionalParameters) {
        synchronized (this) {
            if (closeFuture != null) {
                return CompletableFuture.failedFuture(new IllegalStateException("Connection provider is closed."));
            }
        }

        var latestAuthMillisFuture = new CompletableFuture<Long>();
        var authMapRef = new AtomicReference<AuthToken>();
        return authTokenStageSupplier
                .get()
                .thenCompose(authToken -> {
                    authMapRef.set(authToken);
                    return this.connectionProvider.acquireConnection(
                            address,
                            securityPlan,
                            routingContext,
                            databaseName != null ? databaseName.databaseName().orElse(null) : null,
                            authToken.asMap(),
                            boltAgent,
                            userAgent,
                            mode,
                            connectTimeoutMillis,
                            impersonatedUser,
                            latestAuthMillisFuture,
                            notificationConfig,
                            metricsListener);
                })
                .thenCompose(connection -> {
                    if (minVersion != null
                            && minVersion.compareTo(connection.protocol().version()) > 0) {
                        return connection
                                .close()
                                .thenCompose(
                                        (ignored) -> CompletableFuture.failedStage(new MinVersionAcquisitionException(
                                                "lower version",
                                                connection.protocol().version())));
                    } else {
                        return CompletableFuture.completedStage(connection);
                    }
                })
                .handle((connection, throwable) -> {
                    if (throwable != null) {
                        throwable = FutureUtil.completionExceptionCause(throwable);
                        log.log(System.Logger.Level.DEBUG, "Failed to establish BoltConnection " + address, throwable);
                        throw new CompletionException(throwable);
                    } else {
                        databaseNameConsumer.accept(databaseName);
                        return new BoltConnectionImpl(
                                connection.protocol(),
                                connection,
                                connection.eventLoop(),
                                authMapRef.get(),
                                latestAuthMillisFuture,
                                routingContext,
                                clock,
                                logging,
                                valueFactory);
                    }
                });
    }

    @Override
    public CompletionStage<Void> verifyConnectivity(
            BoltServerAddress address,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            SecurityPlan securityPlan,
            AuthToken authToken) {
        return connect(
                        address,
                        routingContext,
                        boltAgent,
                        userAgent,
                        connectTimeoutMillis,
                        securityPlan,
                        null,
                        () -> CompletableFuture.completedStage(authToken),
                        AccessMode.WRITE,
                        Collections.emptySet(),
                        null,
                        null,
                        null,
                        (ignored) -> {},
                        Collections.emptyMap())
                .thenCompose(BoltConnection::close);
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDb(
            BoltServerAddress address,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            SecurityPlan securityPlan,
            AuthToken authToken) {
        return connect(
                        address,
                        routingContext,
                        boltAgent,
                        userAgent,
                        connectTimeoutMillis,
                        securityPlan,
                        null,
                        () -> CompletableFuture.completedStage(authToken),
                        AccessMode.WRITE,
                        Collections.emptySet(),
                        null,
                        null,
                        null,
                        (ignored) -> {},
                        Collections.emptyMap())
                .thenCompose(boltConnection -> {
                    var supports = boltConnection.protocolVersion().compareTo(BoltProtocolV4.VERSION) >= 0;
                    return boltConnection.close().thenApply(ignored -> supports);
                });
    }

    @Override
    public CompletionStage<Boolean> supportsSessionAuth(
            BoltServerAddress address,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            SecurityPlan securityPlan,
            AuthToken authToken) {
        return connect(
                        address,
                        routingContext,
                        boltAgent,
                        userAgent,
                        connectTimeoutMillis,
                        securityPlan,
                        null,
                        () -> CompletableFuture.completedStage(authToken),
                        AccessMode.WRITE,
                        Collections.emptySet(),
                        null,
                        null,
                        null,
                        (ignored) -> {},
                        Collections.emptyMap())
                .thenCompose(boltConnection -> {
                    var supports = BoltProtocolV51.VERSION.compareTo(boltConnection.protocolVersion()) <= 0;
                    return boltConnection.close().thenApply(ignored -> supports);
                });
    }

    @Override
    public CompletionStage<Void> close() {
        CompletableFuture<Void> closeFuture;
        synchronized (this) {
            if (this.closeFuture == null) {
                this.closeFuture = CompletableFuture.completedFuture(null);
            }
            closeFuture = this.closeFuture;
        }
        return closeFuture;
    }
}
