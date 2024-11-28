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

import static java.util.Objects.requireNonNull;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.socket.nio.NioDomainSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.resolver.AddressResolverGroup;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnixDomainSocketAddress;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltAgent;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.DomainNameResolver;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.MetricsListener;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.RoutingContext;
import org.neo4j.driver.internal.bolt.api.SecurityPlan;
import org.neo4j.driver.internal.bolt.basicimpl.async.NetworkConnection;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelConnectedListener;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelPipelineBuilderImpl;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.NettyChannelInitializer;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.NettyDomainNameResolverGroup;
import org.neo4j.driver.internal.bolt.basicimpl.async.inbound.ConnectTimeoutHandler;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.BoltProtocol;
import org.neo4j.driver.internal.bolt.basicimpl.spi.Connection;

public final class NettyConnectionProvider {
    private final EventLoopGroup eventLoopGroup;
    private final Clock clock;
    private final DomainNameResolver domainNameResolver;
    private final AddressResolverGroup<InetSocketAddress> addressResolverGroup;
    private final LocalAddress localAddress;

    private final LoggingProvider logging;

    public NettyConnectionProvider(
            EventLoopGroup eventLoopGroup,
            Clock clock,
            DomainNameResolver domainNameResolver,
            LocalAddress localAddress,
            LoggingProvider logging) {
        this.eventLoopGroup = eventLoopGroup;
        this.clock = requireNonNull(clock);
        this.domainNameResolver = requireNonNull(domainNameResolver);
        this.addressResolverGroup = new NettyDomainNameResolverGroup(this.domainNameResolver);
        this.localAddress = localAddress;
        this.logging = logging;
    }

    public CompletionStage<Connection> acquireConnection(
            BoltServerAddress address,
            SecurityPlan securityPlan,
            RoutingContext routingContext,
            String databaseName,
            Map<String, Value> authMap,
            BoltAgent boltAgent,
            String userAgent,
            AccessMode mode,
            int connectTimeoutMillis,
            String impersonatedUser,
            CompletableFuture<Long> latestAuthMillisFuture,
            NotificationConfig notificationConfig,
            MetricsListener metricsListener) {

        return installChannelConnectedListeners(
                        address, connect(address, securityPlan, connectTimeoutMillis), connectTimeoutMillis)
                .thenCompose(channel -> BoltProtocol.forChannel(channel)
                        .initializeChannel(
                                channel,
                                requireNonNull(userAgent),
                                requireNonNull(boltAgent),
                                authMap,
                                routingContext,
                                notificationConfig,
                                clock,
                                latestAuthMillisFuture))
                .thenApply(channel -> new NetworkConnection(channel, logging));
    }

    private ChannelFuture connect(BoltServerAddress address, SecurityPlan securityPlan, int connectTimeoutMillis) {
        Class<? extends Channel> channelClass;
        SocketAddress socketAddress;

        if (localAddress != null) {
            channelClass = LocalChannel.class;
            socketAddress = localAddress;
        } else {
            if (address.path() != null) {
                channelClass = NioDomainSocketChannel.class;
                socketAddress = UnixDomainSocketAddress.of(address.path());
            } else {
                channelClass = NioSocketChannel.class;
                try {
                    socketAddress = new InetSocketAddress(
                            domainNameResolver.resolve(address.connectionHost())[0], address.port());
                } catch (Throwable t) {
                    socketAddress = InetSocketAddress.createUnresolved(address.connectionHost(), address.port());
                }
            }
        }

        var bootstrap = new Bootstrap();
        bootstrap
                .group(this.eventLoopGroup)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis)
                .channel(channelClass)
                .resolver(addressResolverGroup)
                .handler(new NettyChannelInitializer(address, securityPlan, connectTimeoutMillis, clock, logging));

        return bootstrap.connect(socketAddress);
    }

    private CompletionStage<Channel> installChannelConnectedListeners(
            BoltServerAddress address, ChannelFuture channelConnected, int connectTimeoutMillis) {
        var pipeline = channelConnected.channel().pipeline();

        // add timeout handler to the pipeline when channel is connected. it's needed to
        // limit amount of time code
        // spends in TLS and Bolt handshakes. prevents infinite waiting when database does
        // not respond
        channelConnected.addListener(future -> pipeline.addFirst(new ConnectTimeoutHandler(connectTimeoutMillis)));

        // add listener that sends Bolt handshake bytes when channel is connected
        var handshakeCompleted = new CompletableFuture<Channel>();
        channelConnected.addListener(
                new ChannelConnectedListener(address, new ChannelPipelineBuilderImpl(), handshakeCompleted, logging));
        return handshakeCompleted.whenComplete((channel, throwable) -> {
            if (throwable == null) {
                // remove timeout handler from the pipeline once TLS and Bolt handshakes are
                // completed. regular protocol
                // messages will flow next and we do not want to have read timeout for them
                channel.pipeline().remove(ConnectTimeoutHandler.class);
            }
        });
    }
}
