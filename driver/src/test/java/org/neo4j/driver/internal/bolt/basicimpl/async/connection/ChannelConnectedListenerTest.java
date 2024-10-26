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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.bolt.api.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.BoltProtocolUtil.handshakeBuf;
import static org.neo4j.driver.testutil.TestUtil.await;

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.bolt.NoopLoggingProvider;

class ChannelConnectedListenerTest {
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldFailPromiseWhenChannelConnectionFails() {
        var handshakeCompletedFuture = new CompletableFuture<Channel>();
        var listener = newListener(handshakeCompletedFuture);

        var channelConnectedPromise = channel.newPromise();
        var cause = new IOException("Unable to connect!");
        channelConnectedPromise.setFailure(cause);

        listener.operationComplete(channelConnectedPromise);

        var error = assertThrows(ServiceUnavailableException.class, () -> await(handshakeCompletedFuture));
        assertEquals(cause, error.getCause());
    }

    @Test
    void shouldWriteHandshakeWhenChannelConnected() {
        var handshakeCompletedFuture = new CompletableFuture<Channel>();
        var listener = newListener(handshakeCompletedFuture);

        var channelConnectedPromise = channel.newPromise();
        channelConnectedPromise.setSuccess();

        listener.operationComplete(channelConnectedPromise);

        assertNotNull(channel.pipeline().get(HandshakeHandler.class));
        assertTrue(channel.finish());
        assertEquals(handshakeBuf(), channel.readOutbound());
    }

    @Test
    void shouldCompleteHandshakePromiseExceptionallyOnWriteFailure() {
        var handshakeCompletedFuture = new CompletableFuture<Channel>();
        var listener = newListener(handshakeCompletedFuture);
        var channelConnectedPromise = channel.newPromise();
        channelConnectedPromise.setSuccess();
        channel.close();

        listener.operationComplete(channelConnectedPromise);

        assertTrue(handshakeCompletedFuture.isCompletedExceptionally());
        var exception = assertThrows(CompletionException.class, handshakeCompletedFuture::join);
        assertInstanceOf(ServiceUnavailableException.class, exception.getCause());
    }

    private static ChannelConnectedListener newListener(CompletableFuture<Channel> handshakeCompletedPromise) {
        return new ChannelConnectedListener(
                LOCAL_DEFAULT,
                new ChannelPipelineBuilderImpl(),
                handshakeCompletedPromise,
                NoopLoggingProvider.INSTANCE);
    }
}
