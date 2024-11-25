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
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.bolt.api.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.BoltProtocolUtil.handshakeBuf;
import static org.neo4j.driver.testutil.TestUtil.await;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.bolt.NoopLoggingProvider;
import org.neo4j.driver.internal.bolt.api.exception.BoltServiceUnavailableException;

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

        var error = assertThrows(BoltServiceUnavailableException.class, () -> await(handshakeCompletedFuture));
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
        assertInstanceOf(BoltServiceUnavailableException.class, exception.getCause());
    }

    @Test
    void shouldCompleteFutureExceptionallyOnFailedPromise() {
        var future = new CompletableFuture<Channel>();
        var listener = newListener(future);
        var throwable = mock(Throwable.class);

        listener.operationComplete(new FailedPromise(throwable));

        assertTrue(future.isCompletedExceptionally());
        Throwable exception = assertThrows(CompletionException.class, future::join);
        assertInstanceOf(BoltServiceUnavailableException.class, exception.getCause());
        exception = exception.getCause();
        assertEquals(throwable, exception.getCause());
    }

    private static ChannelConnectedListener newListener(CompletableFuture<Channel> handshakeCompletedFuture) {
        return new ChannelConnectedListener(
                LOCAL_DEFAULT,
                new ChannelPipelineBuilderImpl(),
                handshakeCompletedFuture,
                NoopLoggingProvider.INSTANCE);
    }

    private record FailedPromise(Throwable failure) implements ChannelPromise {
        @Override
        public Channel channel() {
            return null;
        }

        @Override
        public ChannelPromise setSuccess(Void result) {
            return null;
        }

        @Override
        public boolean trySuccess(Void result) {
            return false;
        }

        @Override
        public ChannelPromise setSuccess() {
            return null;
        }

        @Override
        public boolean trySuccess() {
            return false;
        }

        @Override
        public ChannelPromise setFailure(Throwable cause) {
            return null;
        }

        @Override
        public boolean tryFailure(Throwable cause) {
            return false;
        }

        @Override
        public boolean setUncancellable() {
            return false;
        }

        @Override
        public boolean isSuccess() {
            return false;
        }

        @Override
        public boolean isCancellable() {
            return false;
        }

        @Override
        public Throwable cause() {
            return failure;
        }

        @Override
        public ChannelPromise addListener(GenericFutureListener<? extends Future<? super Void>> listener) {
            return null;
        }

        @Override
        @SafeVarargs
        public final ChannelPromise addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
            return null;
        }

        @Override
        public ChannelPromise removeListener(GenericFutureListener<? extends Future<? super Void>> listener) {
            return null;
        }

        @Override
        @SafeVarargs
        public final ChannelPromise removeListeners(
                GenericFutureListener<? extends Future<? super Void>>... listeners) {
            return null;
        }

        @Override
        public ChannelPromise sync() {
            return null;
        }

        @Override
        public ChannelPromise syncUninterruptibly() {
            return null;
        }

        @Override
        public ChannelPromise await() {
            return null;
        }

        @Override
        public ChannelPromise awaitUninterruptibly() {
            return null;
        }

        @Override
        public boolean await(long timeout, TimeUnit unit) {
            return false;
        }

        @Override
        public boolean await(long timeoutMillis) {
            return false;
        }

        @Override
        public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
            return false;
        }

        @Override
        public boolean awaitUninterruptibly(long timeoutMillis) {
            return false;
        }

        @Override
        public Void getNow() {
            return null;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public Void get() {
            return null;
        }

        @Override
        public Void get(long timeout, @NotNull TimeUnit unit) {
            return null;
        }

        @Override
        public boolean isVoid() {
            return false;
        }

        @Override
        public ChannelPromise unvoid() {
            return null;
        }
    }
}
