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
package org.neo4j.driver.internal.bolt.basicimpl.impl.async.connection;

import static io.netty.buffer.Unpooled.copyInt;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.bolt.basicimpl.impl.async.connection.BoltProtocolUtil.NO_PROTOCOL_VERSION;
import static org.neo4j.driver.internal.bolt.basicimpl.impl.async.connection.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.bolt.basicimpl.impl.util.TestUtil.await;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import javax.net.ssl.SSLHandshakeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.exception.BoltClientException;
import org.neo4j.driver.internal.bolt.api.exception.BoltServiceUnavailableException;
import org.neo4j.driver.internal.bolt.api.values.ValueFactory;
import org.neo4j.driver.internal.bolt.basicimpl.impl.NoopLoggingProvider;
import org.neo4j.driver.internal.bolt.basicimpl.impl.async.inbound.ChunkDecoder;
import org.neo4j.driver.internal.bolt.basicimpl.impl.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.bolt.basicimpl.impl.async.inbound.InboundMessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.impl.async.inbound.MessageDecoder;
import org.neo4j.driver.internal.bolt.basicimpl.impl.async.outbound.OutboundMessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.MessageFormat;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v3.MessageFormatV3;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v4.MessageFormatV4;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v42.BoltProtocolV42;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v43.BoltProtocolV43;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v43.MessageFormatV43;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v44.BoltProtocolV44;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v44.MessageFormatV44;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v5.BoltProtocolV5;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v5.MessageFormatV5;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v51.BoltProtocolV51;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v51.MessageFormatV51;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v52.BoltProtocolV52;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v53.BoltProtocolV53;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v54.BoltProtocolV54;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v54.MessageFormatV54;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v55.BoltProtocolV55;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v56.BoltProtocolV56;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v57.BoltProtocolV57;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v57.MessageFormatV57;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v58.BoltProtocolV58;

class HandshakeHandlerTest {
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @BeforeEach
    void setUp() {
        setMessageDispatcher(channel, new InboundMessageDispatcher(channel, NoopLoggingProvider.INSTANCE));
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldFailGivenPromiseWhenExceptionCaught() {
        var handshakeCompletedFuture = new CompletableFuture<Channel>();
        var handler = newHandler(handshakeCompletedFuture);
        channel.pipeline().addLast(handler);

        var cause = new RuntimeException("Error!");
        channel.pipeline().fireExceptionCaught(cause);

        // promise should fail
        var error = assertThrows(BoltServiceUnavailableException.class, () -> await(handshakeCompletedFuture));
        assertEquals(cause, error.getCause());

        // channel should be closed
        assertNull(await(channel.closeFuture()));
    }

    @Test
    void shouldFailGivenPromiseWhenServiceUnavailableExceptionCaught() {
        var handshakeCompletedFuture = new CompletableFuture<Channel>();
        var handler = newHandler(handshakeCompletedFuture);
        channel.pipeline().addLast(handler);

        var error = new BoltServiceUnavailableException("Bad error");
        channel.pipeline().fireExceptionCaught(error);

        // promise should fail
        var e = assertThrows(BoltServiceUnavailableException.class, () -> await(handshakeCompletedFuture));
        assertEquals(error, e);

        // channel should be closed
        assertNull(await(channel.closeFuture()));
    }

    @Test
    void shouldFailGivenPromiseWhenMultipleExceptionsCaught() {
        var handshakeCompletedFuture = new CompletableFuture<Channel>();
        var handler = newHandler(handshakeCompletedFuture);
        channel.pipeline().addLast(handler);

        var error1 = new RuntimeException("Error 1");
        var error2 = new RuntimeException("Error 2");
        channel.pipeline().fireExceptionCaught(error1);
        channel.pipeline().fireExceptionCaught(error2);

        // promise should fail
        var e1 = assertThrows(BoltServiceUnavailableException.class, () -> await(handshakeCompletedFuture));
        assertEquals(error1, e1.getCause());

        // channel should be closed
        assertNull(await(channel.closeFuture()));

        var e2 = assertThrows(RuntimeException.class, channel::checkException);
        assertEquals(error2, e2);
    }

    @Test
    void shouldUnwrapDecoderException() {
        var handshakeCompletedFuture = new CompletableFuture<Channel>();
        var handler = newHandler(handshakeCompletedFuture);
        channel.pipeline().addLast(handler);

        var cause = new IOException("Error!");
        channel.pipeline().fireExceptionCaught(new DecoderException(cause));

        // promise should fail
        var error = assertThrows(BoltServiceUnavailableException.class, () -> await(handshakeCompletedFuture));
        assertEquals(cause, error.getCause());

        // channel should be closed
        assertNull(await(channel.closeFuture()));
    }

    @Test
    void shouldHandleDecoderExceptionWithoutCause() {
        var handshakeCompletedFuture = new CompletableFuture<Channel>();
        var handler = newHandler(handshakeCompletedFuture);
        channel.pipeline().addLast(handler);

        var decoderException = new DecoderException("Unable to decode a message");
        channel.pipeline().fireExceptionCaught(decoderException);

        var error = assertThrows(BoltServiceUnavailableException.class, () -> await(handshakeCompletedFuture));
        assertEquals(decoderException, error.getCause());

        // channel should be closed
        assertNull(await(channel.closeFuture()));
    }

    @Test
    void shouldNotTranslateSSLHandshakeException() {
        var handshakeCompletedFuture = new CompletableFuture<Channel>();
        var handler = newHandler(handshakeCompletedFuture);
        channel.pipeline().addLast(handler);

        var error = new SSLHandshakeException("Invalid certificate");
        channel.pipeline().fireExceptionCaught(error);

        // promise should fail
        var e = assertThrows(SSLHandshakeException.class, () -> await(handshakeCompletedFuture));
        assertEquals(error, e);

        // channel should be closed
        assertNull(await(channel.closeFuture()));
    }

    @ParameterizedTest
    @MethodSource("protocolVersions")
    public void testProtocolSelection(
            BoltProtocolVersion protocolVersion, Class<? extends MessageFormat> expectedMessageFormatClass) {
        var handshakeCompletedFuture = new CompletableFuture<Channel>();
        var pipelineBuilder = new MemorizingChannelPipelineBuilder();
        var handler = newHandler(pipelineBuilder, handshakeCompletedFuture);
        channel.pipeline().addLast(handler);

        channel.pipeline().fireChannelRead(copyInt(protocolVersion.toInt()));

        // expected message format should've been used
        assertInstanceOf(expectedMessageFormatClass, pipelineBuilder.usedMessageFormat);

        // handshake handler itself should be removed
        assertNull(channel.pipeline().get(HandshakeHandler.class));

        // all inbound handlers should be set
        assertNotNull(channel.pipeline().get(ChunkDecoder.class));
        assertNotNull(channel.pipeline().get(MessageDecoder.class));
        assertNotNull(channel.pipeline().get(InboundMessageHandler.class));

        // all outbound handlers should be set
        assertNotNull(channel.pipeline().get(OutboundMessageHandler.class));

        // promise should be successful
        assertEquals(channel, await(handshakeCompletedFuture));
    }

    @Test
    void shouldFailGivenPromiseWhenServerSuggestsNoProtocol() {
        testFailure(NO_PROTOCOL_VERSION, "The server does not support any of the protocol versions");
    }

    @Test
    void shouldFailGivenPromiseWhenServerSuggestsHttp() {
        testFailure(new BoltProtocolVersion(80, 84), "Server responded HTTP");
    }

    @Test
    void shouldFailGivenPromiseWhenServerSuggestsUnknownProtocol() {
        testFailure(new BoltProtocolVersion(42, 0), "Protocol error");
    }

    @Test
    void shouldFailGivenPromiseWhenChannelInactive() {
        var handshakeCompletedFuture = new CompletableFuture<Channel>();
        var handler = newHandler(handshakeCompletedFuture);
        channel.pipeline().addLast(handler);

        channel.pipeline().fireChannelInactive();

        // promise should fail
        var error = assertThrows(BoltServiceUnavailableException.class, () -> await(handshakeCompletedFuture));
        assertEquals(
                "Connection to the database terminated. Please ensure that your database is listening on the correct host and port and that you have compatible encryption settings both on Neo4j server and driver. Note that the default encryption setting has changed in Neo4j 4.0.",
                error.getMessage());

        // channel should be closed
        assertNull(await(channel.closeFuture()));
    }

    private void testFailure(BoltProtocolVersion serverSuggestedVersion, String expectedMessagePrefix) {
        var handshakeCompletedFuture = new CompletableFuture<Channel>();
        var handler = newHandler(handshakeCompletedFuture);
        channel.pipeline().addLast(handler);

        channel.pipeline().fireChannelRead(copyInt(serverSuggestedVersion.toInt()));

        // handshake handler itself should be removed
        assertNull(channel.pipeline().get(HandshakeHandler.class));

        // promise should fail
        var error = assertThrows(Exception.class, () -> await(handshakeCompletedFuture));
        assertInstanceOf(BoltClientException.class, error);
        assertTrue(error.getMessage().startsWith(expectedMessagePrefix));

        // channel should be closed
        assertNull(await(channel.closeFuture()));
    }

    private static Stream<Arguments> protocolVersions() {
        return Stream.of(
                arguments(BoltProtocolV3.VERSION, MessageFormatV3.class),
                arguments(BoltProtocolV42.VERSION, MessageFormatV4.class),
                arguments(BoltProtocolV43.VERSION, MessageFormatV43.class),
                arguments(BoltProtocolV44.VERSION, MessageFormatV44.class),
                arguments(BoltProtocolV5.VERSION, MessageFormatV5.class),
                arguments(BoltProtocolV51.VERSION, MessageFormatV51.class),
                arguments(BoltProtocolV52.VERSION, MessageFormatV51.class),
                arguments(BoltProtocolV53.VERSION, MessageFormatV51.class),
                arguments(BoltProtocolV54.VERSION, MessageFormatV54.class),
                arguments(BoltProtocolV55.VERSION, MessageFormatV54.class),
                arguments(BoltProtocolV56.VERSION, MessageFormatV54.class),
                arguments(BoltProtocolV57.VERSION, MessageFormatV57.class),
                arguments(BoltProtocolV58.VERSION, MessageFormatV57.class));
    }

    private static HandshakeHandler newHandler(CompletableFuture<Channel> handshakeCompletedFuture) {
        return newHandler(new ChannelPipelineBuilderImpl(), handshakeCompletedFuture);
    }

    private static HandshakeHandler newHandler(
            ChannelPipelineBuilder pipelineBuilder, CompletableFuture<Channel> handshakeCompletedPromise) {
        return new HandshakeHandler(
                pipelineBuilder, handshakeCompletedPromise, NoopLoggingProvider.INSTANCE, mock(ValueFactory.class));
    }

    private static class MemorizingChannelPipelineBuilder extends ChannelPipelineBuilderImpl {
        MessageFormat usedMessageFormat;

        @Override
        public void build(
                MessageFormat messageFormat,
                ChannelPipeline pipeline,
                LoggingProvider logging,
                ValueFactory valueFactory) {
            usedMessageFormat = messageFormat;
            super.build(messageFormat, pipeline, logging, valueFactory);
        }
    }
}
