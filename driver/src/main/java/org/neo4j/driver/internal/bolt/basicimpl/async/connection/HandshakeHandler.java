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

import static org.neo4j.driver.internal.bolt.api.BoltProtocolVersion.isHttp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.ReplayingDecoder;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.net.ssl.SSLHandshakeException;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.exception.BoltClientException;
import org.neo4j.driver.internal.bolt.api.exception.BoltServiceUnavailableException;
import org.neo4j.driver.internal.bolt.basicimpl.logging.ChannelActivityLogger;
import org.neo4j.driver.internal.bolt.basicimpl.logging.ChannelErrorLogger;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.BoltProtocol;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageFormat;

public class HandshakeHandler extends ReplayingDecoder<Void> {
    private final ChannelPipelineBuilder pipelineBuilder;
    private final CompletableFuture<Channel> handshakeCompletedFuture;
    private final LoggingProvider logging;

    private boolean failed;
    private ChannelActivityLogger log;
    private ChannelErrorLogger errorLog;

    public HandshakeHandler(
            ChannelPipelineBuilder pipelineBuilder,
            CompletableFuture<Channel> handshakeCompletedFuture,
            LoggingProvider logging) {
        this.pipelineBuilder = pipelineBuilder;
        this.handshakeCompletedFuture = handshakeCompletedFuture;
        this.logging = logging;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        log = new ChannelActivityLogger(ctx.channel(), logging, getClass());
        errorLog = new ChannelErrorLogger(ctx.channel(), logging);
    }

    @Override
    protected void handlerRemoved0(ChannelHandlerContext ctx) {
        failed = false;
        log = null;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.log(System.Logger.Level.DEBUG, "Channel is inactive");

        if (!failed) {
            // channel became inactive while doing bolt handshake, not because of some previous error
            var error = newConnectionTerminatedError();
            fail(ctx, error);
        }
    }

    public static BoltServiceUnavailableException newConnectionTerminatedError() {
        return new BoltServiceUnavailableException("Connection to the database terminated. "
                + "Please ensure that your database is listening on the correct host and port and that you have compatible encryption settings both on Neo4j server and driver. "
                + "Note that the default encryption setting has changed in Neo4j 4.0.");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable error) {
        if (failed) {
            errorLog.traceOrDebug("Another fatal error occurred in the pipeline", error);
        } else {
            failed = true;
            var cause = transformError(error);
            fail(ctx, cause);
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        var serverSuggestedVersion = BoltProtocolVersion.fromRawBytes(in.readInt());
        log.log(System.Logger.Level.DEBUG, "S: [Bolt Handshake] %s", serverSuggestedVersion);

        // this is a one-time handler, remove it when protocol version has been read
        ctx.pipeline().remove(this);

        var protocol = protocolForVersion(serverSuggestedVersion);
        if (protocol != null) {
            protocolSelected(serverSuggestedVersion, protocol.createMessageFormat(), ctx);
        } else {
            handleUnknownSuggestedProtocolVersion(serverSuggestedVersion, ctx);
        }
    }

    private BoltProtocol protocolForVersion(BoltProtocolVersion version) {
        try {
            return BoltProtocol.forVersion(version);
        } catch (BoltClientException e) {
            return null;
        }
    }

    private void protocolSelected(BoltProtocolVersion version, MessageFormat messageFormat, ChannelHandlerContext ctx) {
        ChannelAttributes.setProtocolVersion(ctx.channel(), version);
        pipelineBuilder.build(messageFormat, ctx.pipeline(), logging);
        handshakeCompletedFuture.complete(ctx.channel());
    }

    private void handleUnknownSuggestedProtocolVersion(BoltProtocolVersion version, ChannelHandlerContext ctx) {
        if (BoltProtocolUtil.NO_PROTOCOL_VERSION.equals(version)) {
            fail(ctx, protocolNoSupportedByServerError());
        } else if (isHttp(version)) {
            fail(ctx, httpEndpointError());
        } else {
            fail(ctx, protocolNoSupportedByDriverError(version));
        }
    }

    private void fail(ChannelHandlerContext ctx, Throwable error) {
        ctx.close().addListener(future -> handshakeCompletedFuture.completeExceptionally(error));
    }

    private static Throwable protocolNoSupportedByServerError() {
        return new BoltClientException("The server does not support any of the protocol versions supported by "
                + "this driver. Ensure that you are using driver and server versions that "
                + "are compatible with one another.");
    }

    private static Throwable httpEndpointError() {
        return new BoltClientException(
                "Server responded HTTP. Make sure you are not trying to connect to the http endpoint "
                        + "(HTTP defaults to port 7474 whereas BOLT defaults to port 7687)");
    }

    private static Throwable protocolNoSupportedByDriverError(BoltProtocolVersion suggestedProtocolVersion) {
        return new BoltClientException(
                "Protocol error, server suggested unexpected protocol version: " + suggestedProtocolVersion);
    }

    private static Throwable transformError(Throwable error) {
        if (error instanceof DecoderException && error.getCause() != null) {
            // unwrap the DecoderException if it has a cause
            error = error.getCause();
        }

        if (error instanceof BoltServiceUnavailableException || error instanceof SSLHandshakeException) {
            return error;
        } else {
            return new BoltServiceUnavailableException("Failed to establish connection with the server", error);
        }
    }
}
