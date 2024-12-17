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
package org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v43;

import java.time.Clock;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.ClusterComposition;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.summary.RouteSummary;
import org.neo4j.driver.internal.bolt.api.values.Value;
import org.neo4j.driver.internal.bolt.api.values.ValueFactory;
import org.neo4j.driver.internal.bolt.basicimpl.impl.handlers.RouteMessageResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.BoltProtocol;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.MessageFormat;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.MessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.request.RouteMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v42.BoltProtocolV42;
import org.neo4j.driver.internal.bolt.basicimpl.impl.spi.Connection;

/**
 * Definition of the Bolt Protocol 4.3
 * <p>
 * The version 4.3 use most of the 4.2 behaviours, but it extends it with new messages such as ROUTE
 */
public class BoltProtocolV43 extends BoltProtocolV42 {
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion(4, 3);
    public static final BoltProtocol INSTANCE = new BoltProtocolV43();

    @Override
    public MessageFormat createMessageFormat() {
        return new MessageFormatV43();
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public CompletionStage<Void> route(
            Connection connection,
            Map<String, Value> routingContext,
            Set<String> bookmarks,
            String databaseName,
            String impersonatedUser,
            MessageHandler<RouteSummary> handler,
            Clock clock,
            LoggingProvider logging,
            ValueFactory valueFactory) {
        var routeMessage = new RouteMessage(routingContext, bookmarks, databaseName, impersonatedUser);
        var routeFuture = new CompletableFuture<Map<String, Value>>();
        routeFuture
                .thenApply(map -> {
                    var ttl = map.get("ttl").asLong();
                    var expirationTimestamp = clock.millis() + ttl * 1000;
                    if (ttl < 0 || ttl >= Long.MAX_VALUE / 1000L || expirationTimestamp < 0) {
                        expirationTimestamp = Long.MAX_VALUE;
                    }

                    Set<BoltServerAddress> readers = new LinkedHashSet<>();
                    Set<BoltServerAddress> writers = new LinkedHashSet<>();
                    Set<BoltServerAddress> routers = new LinkedHashSet<>();

                    for (var serversMap : map.get("servers").values()) {
                        var role = serversMap.get("role").asString();
                        for (var server : serversMap.get("addresses").values()) {
                            var address = new BoltServerAddress(server.asString());
                            switch (role) {
                                case "WRITE" -> writers.add(address);
                                case "READ" -> readers.add(address);
                                case "ROUTE" -> routers.add(address);
                            }
                        }
                    }
                    var db = map.get("db");
                    String name = null;
                    if (db != null && !db.isNull()) {
                        name = db.asString();
                    }

                    var clusterComposition =
                            new ClusterComposition(expirationTimestamp, readers, writers, routers, name);
                    return new RouteSummaryImpl(clusterComposition);
                })
                .whenComplete((summary, throwable) -> {
                    if (throwable != null) {
                        handler.onError(throwable);
                    } else {
                        handler.onSummary(summary);
                    }
                });
        var routeHandler = new RouteMessageResponseHandler(routeFuture, valueFactory);
        return connection.write(routeMessage, routeHandler);
    }

    @Override
    public BoltProtocolVersion version() {
        return VERSION;
    }

    @Override
    protected boolean includeDateTimeUtcPatchInHello() {
        return true;
    }

    private record RouteSummaryImpl(ClusterComposition clusterComposition) implements RouteSummary {}
}
