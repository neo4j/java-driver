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
package org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v51;

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
import org.neo4j.driver.internal.bolt.basicimpl.impl.handlers.LogoffResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.impl.handlers.LogonResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.BoltProtocol;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.MessageFormat;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.MessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.request.HelloMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.request.LogoffMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.request.LogonMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v5.BoltProtocolV5;
import org.neo4j.driver.internal.bolt.basicimpl.impl.spi.Connection;

public class BoltProtocolV51 extends BoltProtocolV5 {
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion(5, 1);
    public static final BoltProtocol INSTANCE = new BoltProtocolV51();

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
                    null,
                    Collections.emptyMap(),
                    routingContext.toMap(),
                    false,
                    notificationConfig,
                    useLegacyNotifications(),
                    valueFactory);
        } else {
            message = new HelloMessage(
                    userAgent,
                    null,
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
    public CompletionStage<Void> logoff(Connection connection, MessageHandler<Void> handler) {
        var logoffMessage = LogoffMessage.INSTANCE;
        var logoffFuture = new CompletableFuture<Void>();
        logoffFuture.whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                handler.onError(throwable);
            } else {
                handler.onSummary(null);
            }
        });
        var logoffHandler = new LogoffResponseHandler(logoffFuture);
        return connection.write(logoffMessage, logoffHandler);
    }

    @Override
    public CompletionStage<Void> logon(
            Connection connection,
            Map<String, Value> authMap,
            Clock clock,
            MessageHandler<Void> handler,
            ValueFactory valueFactory) {
        var logonMessage = new LogonMessage(authMap, valueFactory);
        var logonFuture = new CompletableFuture<Long>();
        logonFuture.whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                handler.onError(throwable);
            } else {
                handler.onSummary(null);
            }
        });
        var logonHandler = new LogonResponseHandler(logonFuture, null, clock, logonFuture);
        return connection.write(logonMessage, logonHandler);
    }

    @Override
    public BoltProtocolVersion version() {
        return VERSION;
    }

    @Override
    public MessageFormat createMessageFormat() {
        return new MessageFormatV51();
    }
}
