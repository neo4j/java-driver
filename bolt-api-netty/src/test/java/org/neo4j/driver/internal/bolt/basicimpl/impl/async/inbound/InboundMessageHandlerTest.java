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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.request.ResetMessage.RESET;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.neo4j.bolt.api.test.values.TestValueFactory;
import org.neo4j.driver.internal.bolt.api.exception.BoltFailureException;
import org.neo4j.driver.internal.bolt.api.values.Value;
import org.neo4j.driver.internal.bolt.api.values.ValueFactory;
import org.neo4j.driver.internal.bolt.basicimpl.impl.NoopLoggingProvider;
import org.neo4j.driver.internal.bolt.basicimpl.impl.async.connection.ChannelAttributes;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.MessageFormat;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.response.FailureMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.response.IgnoredMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.response.RecordMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.response.SuccessMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v3.MessageFormatV3;
import org.neo4j.driver.internal.bolt.basicimpl.impl.spi.ResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.impl.util.io.MessageToByteBufWriter;
import org.neo4j.driver.internal.bolt.basicimpl.impl.util.messaging.KnowledgeableMessageFormat;

class InboundMessageHandlerTest {
    private static final ValueFactory valueFactory = TestValueFactory.INSTANCE;
    private EmbeddedChannel channel;
    private InboundMessageDispatcher messageDispatcher;
    private MessageToByteBufWriter writer;

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel();
        messageDispatcher = new InboundMessageDispatcher(channel, NoopLoggingProvider.INSTANCE);
        writer = new MessageToByteBufWriter(new KnowledgeableMessageFormat(false));
        ChannelAttributes.setMessageDispatcher(channel, messageDispatcher);

        var handler = new InboundMessageHandler(new MessageFormatV3(), NoopLoggingProvider.INSTANCE, valueFactory);
        channel.pipeline().addFirst(handler);
    }

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldReadSuccessMessage() {
        var responseHandler = mock(ResponseHandler.class);
        messageDispatcher.enqueue(responseHandler);

        Map<String, Value> metadata = new HashMap<>();
        metadata.put("key1", valueFactory.value(1));
        metadata.put("key2", valueFactory.value(2));
        channel.writeInbound(writer.asByteBuf(new SuccessMessage(metadata)));

        verify(responseHandler).onSuccess(metadata);
    }

    @Test
    void shouldReadFailureMessage() {
        var responseHandler = mock(ResponseHandler.class);
        messageDispatcher.enqueue(responseHandler);

        channel.writeInbound(writer.asByteBuf(new FailureMessage("Neo.TransientError.General.ReadOnly", "Hi!")));

        var captor = ArgumentCaptor.forClass(BoltFailureException.class);
        verify(responseHandler).onFailure(captor.capture());
        assertEquals("Neo.TransientError.General.ReadOnly", captor.getValue().code());
        assertEquals("Hi!", captor.getValue().getMessage());
    }

    @Test
    void shouldReadRecordMessage() {
        var responseHandler = mock(ResponseHandler.class);
        messageDispatcher.enqueue(responseHandler);

        var fields = new Value[] {valueFactory.value(1), valueFactory.value(2), valueFactory.value(3)};
        channel.writeInbound(writer.asByteBuf(new RecordMessage(fields)));

        verify(responseHandler).onRecord(fields);
    }

    @Test
    void shouldReadIgnoredMessage() {
        var responseHandler = mock(ResponseHandler.class);
        messageDispatcher.enqueue(responseHandler);

        channel.writeInbound(writer.asByteBuf(IgnoredMessage.IGNORED));
        assertEquals(0, messageDispatcher.queuedHandlersCount());
    }

    @Test
    void shouldRethrowReadErrors() throws IOException {
        var messageFormat = mock(MessageFormat.class);
        var reader = mock(MessageFormat.Reader.class);
        var error = new RuntimeException("Unable to decode!");
        doThrow(error).when(reader).read(any());
        when(messageFormat.newReader(any(), any())).thenReturn(reader);

        var handler = new InboundMessageHandler(messageFormat, NoopLoggingProvider.INSTANCE, valueFactory);

        channel.pipeline().remove(InboundMessageHandler.class);
        channel.pipeline().addLast(handler);

        var e = assertThrows(DecoderException.class, () -> channel.writeInbound(writer.asByteBuf(RESET)));
        assertTrue(e.getMessage().startsWith("Failed to read inbound message"));
    }
}
