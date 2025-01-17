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
package org.neo4j.driver.internal.bolt.basicimpl.impl.messaging;

import static org.neo4j.driver.internal.bolt.basicimpl.impl.async.connection.ChannelAttributes.protocolVersion;

import io.netty.channel.Channel;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltAgent;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.RoutingContext;
import org.neo4j.driver.internal.bolt.api.exception.BoltClientException;
import org.neo4j.driver.internal.bolt.api.exception.BoltUnsupportedFeatureException;
import org.neo4j.driver.internal.bolt.api.summary.BeginSummary;
import org.neo4j.driver.internal.bolt.api.summary.DiscardSummary;
import org.neo4j.driver.internal.bolt.api.summary.RouteSummary;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;
import org.neo4j.driver.internal.bolt.api.values.Value;
import org.neo4j.driver.internal.bolt.api.values.ValueFactory;
import org.neo4j.driver.internal.bolt.basicimpl.impl.async.connection.BoltProtocolUtil;
import org.neo4j.driver.internal.bolt.basicimpl.impl.spi.Connection;

public interface BoltProtocol {
    MessageFormat createMessageFormat();

    CompletionStage<Channel> initializeChannel(
            Channel channel,
            String userAgent,
            BoltAgent boltAgent,
            Map<String, Value> authMap,
            RoutingContext routingContext,
            NotificationConfig notificationConfig,
            Clock clock,
            CompletableFuture<Long> latestAuthMillisFuture,
            ValueFactory valueFactory);

    CompletionStage<Void> route(
            Connection connection,
            Map<String, Value> routingContext,
            Set<String> bookmarks,
            String databaseName,
            String impersonatedUser,
            MessageHandler<RouteSummary> handler,
            Clock clock,
            LoggingProvider logging,
            ValueFactory valueFactory);

    CompletionStage<Void> beginTransaction(
            Connection connection,
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            Set<String> bookmarks,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            String txType,
            NotificationConfig notificationConfig,
            MessageHandler<BeginSummary> handler,
            LoggingProvider logging,
            ValueFactory valueFactory);

    CompletionStage<Void> commitTransaction(Connection connection, MessageHandler<String> handler);

    CompletionStage<Void> rollbackTransaction(Connection connection, MessageHandler<Void> handler);

    CompletionStage<Void> telemetry(Connection connection, Integer api, MessageHandler<Void> handler);

    CompletionStage<Void> runAuto(
            Connection connection,
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            String query,
            Map<String, Value> parameters,
            Set<String> bookmarks,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            NotificationConfig notificationConfig,
            MessageHandler<RunSummary> handler,
            LoggingProvider logging,
            ValueFactory valueFactory);

    CompletionStage<Void> run(
            Connection connection, String query, Map<String, Value> parameters, MessageHandler<RunSummary> handler);

    CompletionStage<Void> pull(
            Connection connection, long qid, long request, PullMessageHandler handler, ValueFactory valueFactory);

    CompletionStage<Void> discard(
            Connection connection,
            long qid,
            long number,
            MessageHandler<DiscardSummary> handler,
            ValueFactory valueFactory);

    CompletionStage<Void> reset(Connection connection, MessageHandler<Void> handler);

    default CompletionStage<Void> logoff(Connection connection, MessageHandler<Void> handler) {
        return CompletableFuture.failedStage(new BoltUnsupportedFeatureException("logoff not supported"));
    }

    default CompletionStage<Void> logon(
            Connection connection,
            Map<String, Value> authMap,
            Clock clock,
            MessageHandler<Void> handler,
            ValueFactory valueFactory) {
        return CompletableFuture.failedStage(new BoltUnsupportedFeatureException("logon not supported"));
    }

    /**
     * Returns the protocol version. It can be used for version specific error messages.
     * @return the protocol version.
     */
    BoltProtocolVersion version();

    static BoltProtocol forChannel(Channel channel) {
        return forVersion(protocolVersion(channel));
    }

    static BoltProtocol forVersion(BoltProtocolVersion version) {
        var protocol = BoltProtocolUtil.versionToProtocol.get(version);
        if (protocol != null) {
            return protocol;
        } else {
            throw new BoltClientException("Unknown protocol version: " + version);
        }
    }
}
