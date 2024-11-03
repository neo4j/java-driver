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
package org.neo4j.driver.internal.bolt.basicimpl.async.connection;

import static java.lang.String.format;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.BoltProtocolUtil.handshakeString;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import java.util.concurrent.CompletableFuture;
import javax.net.ssl.SSLHandshakeException;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.GqlStatusError;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.exception.SecurityException;
import org.neo4j.driver.internal.bolt.api.exception.ServiceUnavailableException;
import org.neo4j.driver.internal.bolt.basicimpl.logging.ChannelActivityLogger;

public class ChannelConnectedListener implements ChannelFutureListener {
    private final BoltServerAddress address;
    private final ChannelPipelineBuilder pipelineBuilder;
    private final CompletableFuture<Channel> handshakeCompletedFuture;
    private final LoggingProvider logging;

    public ChannelConnectedListener(
            BoltServerAddress address,
            ChannelPipelineBuilder pipelineBuilder,
            CompletableFuture<Channel> handshakeCompletedFuture,
            LoggingProvider logging) {
        this.address = address;
        this.pipelineBuilder = pipelineBuilder;
        this.handshakeCompletedFuture = handshakeCompletedFuture;
        this.logging = logging;
    }

    @Override
    public void operationComplete(ChannelFuture future) {
        if (future.isSuccess()) {
            var channel = future.channel();
            var log = new ChannelActivityLogger(channel, logging, getClass());
            log.log(System.Logger.Level.TRACE, "Channel %s connected, initiating bolt handshake", channel);

            var pipeline = channel.pipeline();
            pipeline.addLast(new HandshakeHandler(pipelineBuilder, handshakeCompletedFuture, logging));
            log.log(System.Logger.Level.DEBUG, "C: [Bolt Handshake] %s", handshakeString());
            channel.writeAndFlush(BoltProtocolUtil.handshakeBuf()).addListener(f -> {
                if (!f.isSuccess()) {
                    var error = f.cause();
                    if (error instanceof SSLHandshakeException) {
                        var message = "Failed to establish secured connection with the server";
                        error = new SecurityException(
                                GqlStatusError.UNKNOWN.getStatus(),
                                GqlStatusError.UNKNOWN.getStatusDescription(message),
                                "N/A",
                                message,
                                GqlStatusError.DIAGNOSTIC_RECORD,
                                error);
                    } else {
                        error = new ServiceUnavailableException(
                                String.format("Unable to write Bolt handshake to %s.", this.address), error);
                    }
                    this.handshakeCompletedFuture.completeExceptionally(error);
                }
            });
        } else {
            handshakeCompletedFuture.completeExceptionally(databaseUnavailableError(address, future.cause()));
        }
    }

    private static Throwable databaseUnavailableError(BoltServerAddress address, Throwable cause) {
        return new ServiceUnavailableException(
                format(
                        "Unable to connect to %s, ensure the database is running and that there "
                                + "is a working network connection to it.",
                        address),
                cause);
    }
}
