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
package org.neo4j.driver.internal.bolt.basicimpl.impl.async.inbound;

import static java.util.Objects.requireNonNull;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.CodecException;
import java.io.IOException;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.exception.BoltConnectionReadTimeoutException;
import org.neo4j.driver.internal.bolt.api.exception.BoltServiceUnavailableException;
import org.neo4j.driver.internal.bolt.basicimpl.impl.async.connection.ChannelAttributes;
import org.neo4j.driver.internal.bolt.basicimpl.impl.logging.ChannelActivityLogger;
import org.neo4j.driver.internal.bolt.basicimpl.impl.logging.ChannelErrorLogger;

public class ChannelErrorHandler extends ChannelInboundHandlerAdapter {
    private final LoggingProvider logging;

    private InboundMessageDispatcher messageDispatcher;
    private ChannelActivityLogger log;
    private ChannelErrorLogger errorLog;
    private boolean failed;

    public ChannelErrorHandler(LoggingProvider logging) {
        this.logging = logging;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        messageDispatcher = requireNonNull(ChannelAttributes.messageDispatcher(ctx.channel()));
        log = new ChannelActivityLogger(ctx.channel(), logging, getClass());
        errorLog = new ChannelErrorLogger(ctx.channel(), logging);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        messageDispatcher = null;
        log = null;
        failed = false;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.log(System.Logger.Level.DEBUG, "Channel is inactive");

        var terminationReason = ChannelAttributes.terminationReason(ctx.channel());
        Throwable error = terminationReason == null
                ? new BoltServiceUnavailableException("Connection to the database terminated. "
                        + "Please ensure that your database is listening on the correct host and port and that you have compatible encryption settings both on Neo4j server and driver. "
                        + "Note that the default encryption setting has changed in Neo4j 4.0.")
                : new BoltServiceUnavailableException("Connection to the database terminated. " + terminationReason);

        if (!failed) {
            // channel became inactive not because of a fatal exception that came from exceptionCaught
            // it is most likely inactive because actual network connection broke or was explicitly closed by the driver

            messageDispatcher.handleChannelInactive(error);
        } else {
            fail(error);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable error) {
        if (failed) {
            errorLog.traceOrDebug("Another fatal error occurred in the pipeline", error);
        } else {
            failed = true;
            logUnexpectedErrorWarning(error);
            fail(error);
        }
    }

    private void logUnexpectedErrorWarning(Throwable error) {
        if (!(error instanceof BoltConnectionReadTimeoutException)) {
            errorLog.traceOrDebug("Fatal error occurred in the pipeline", error);
        }
    }

    private void fail(Throwable error) {
        var cause = transformError(error);
        messageDispatcher.handleChannelError(cause);
    }

    private static Throwable transformError(Throwable error) {
        if (error instanceof CodecException && error.getCause() != null) {
            // unwrap the CodecException if it has a cause
            error = error.getCause();
        }

        if (error instanceof IOException) {
            return new BoltServiceUnavailableException("Connection to the database failed", error);
        } else {
            return error;
        }
    }
}
