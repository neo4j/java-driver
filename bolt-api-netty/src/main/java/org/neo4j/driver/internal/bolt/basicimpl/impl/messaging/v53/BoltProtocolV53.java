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
package org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v53;

import static org.neo4j.driver.internal.bolt.basicimpl.impl.async.connection.ChannelAttributes.messageDispatcher;

import io.netty.channel.Channel;
import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.internal.bolt.api.BoltAgent;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.RoutingContext;
import org.neo4j.driver.internal.bolt.api.values.Value;
import org.neo4j.driver.internal.bolt.api.values.ValueFactory;
import org.neo4j.driver.internal.bolt.basicimpl.impl.handlers.HelloV51ResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.impl.handlers.LogonResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.BoltProtocol;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.request.HelloMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.request.LogonMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v52.BoltProtocolV52;

public class BoltProtocolV53 extends BoltProtocolV52 {
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion(5, 3);
    public static final BoltProtocol INSTANCE = new BoltProtocolV53();

    @SuppressWarnings("DuplicatedCode")
    @Override
    public CompletionStage<Channel> initializeChannel(
            Channel channel,
            String userAgent,
            BoltAgent boltAgent,
            Map<String, Value> authMap,
            RoutingContext routingContext,
            NotificationConfig notificationConfig,
            Clock clock,
            CompletableFuture<Long> latestAuthMillisFuture,
            ValueFactory valueFactory) {
        var exception = verifyNotificationConfigSupported(notificationConfig);
        if (exception != null) {
            return CompletableFuture.failedStage(exception);
        }
        HelloMessage message;

        if (routingContext.isServerRoutingEnabled()) {
            message = new HelloMessage(
                    userAgent,
                    boltAgent,
                    Collections.emptyMap(),
                    routingContext.toMap(),
                    false,
                    notificationConfig,
                    useLegacyNotifications(),
                    valueFactory);
        } else {
            message = new HelloMessage(
                    userAgent,
                    boltAgent,
                    Collections.emptyMap(),
                    null,
                    false,
                    notificationConfig,
                    useLegacyNotifications(),
                    valueFactory);
        }

        var helloFuture = new CompletableFuture<String>();
        messageDispatcher(channel).enqueue(new HelloV51ResponseHandler(channel, helloFuture));
        channel.write(message, channel.voidPromise());

        var logonFuture = new CompletableFuture<Void>();
        var logon = new LogonMessage(authMap, valueFactory);
        messageDispatcher(channel)
                .enqueue(new LogonResponseHandler(logonFuture, channel, clock, latestAuthMillisFuture));
        channel.writeAndFlush(logon, channel.voidPromise());

        return helloFuture.thenCompose(ignored -> logonFuture).thenApply(ignored -> channel);
    }

    @Override
    public BoltProtocolVersion version() {
        return VERSION;
    }
}
