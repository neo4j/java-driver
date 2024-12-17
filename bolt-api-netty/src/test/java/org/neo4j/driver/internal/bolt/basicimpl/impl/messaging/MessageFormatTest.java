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

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.bolt.basicimpl.impl.async.connection.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.bolt.basicimpl.impl.async.connection.ChannelAttributes.setMessageDispatcher;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.api.test.values.TestValueFactory;
import org.neo4j.driver.internal.bolt.api.exception.BoltClientException;
import org.neo4j.driver.internal.bolt.api.values.Value;
import org.neo4j.driver.internal.bolt.basicimpl.impl.NoopLoggingProvider;
import org.neo4j.driver.internal.bolt.basicimpl.impl.async.connection.BoltProtocolUtil;
import org.neo4j.driver.internal.bolt.basicimpl.impl.async.connection.ChannelPipelineBuilderImpl;
import org.neo4j.driver.internal.bolt.basicimpl.impl.async.outbound.ChunkAwareByteBufOutput;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.common.CommonValueUnpacker;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.response.FailureMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.response.IgnoredMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.response.RecordMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.response.SuccessMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v3.MessageFormatV3;
import org.neo4j.driver.internal.bolt.basicimpl.impl.packstream.PackStream;
import org.neo4j.driver.internal.bolt.basicimpl.impl.spi.ResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.impl.util.messaging.KnowledgeableMessageFormat;
import org.neo4j.driver.internal.bolt.basicimpl.impl.util.messaging.MemorizingInboundMessageDispatcher;

class MessageFormatTest {
    private static final TestValueFactory valueFactory = TestValueFactory.INSTANCE;
    public final MessageFormat format = new MessageFormatV3();

    @Test
    void shouldUnpackAllResponses() {
        assertSerializes(new FailureMessage("Hello", "World!"));
        assertSerializes(IgnoredMessage.IGNORED);
        assertSerializes(new RecordMessage(new Value[] {valueFactory.value(1337L)}));
        assertSerializes(new SuccessMessage(new HashMap<>()));
    }

    @Test
    void shouldPackUnpackValidValues() {
        assertSerializesValue(
                valueFactory.value(Map.of("cat", valueFactory.value(null), "dog", valueFactory.value(null))));
        assertSerializesValue(
                valueFactory.value(Map.of("k", valueFactory.value(12), "a", valueFactory.value("banana"))));
        assertSerializesValue(valueFactory.value(asList("k", 12, "a", "banana")));
    }

    @Test
    void shouldUnpackNodeRelationshipAndPath() {
        // Given
        assertOnlyDeserializesValue(valueFactory.emptyNodeValue());
        assertOnlyDeserializesValue(valueFactory.filledNodeValue());
        assertOnlyDeserializesValue(valueFactory.emptyRelationshipValue());
        assertOnlyDeserializesValue(valueFactory.filledRelationshipValue());
        assertOnlyDeserializesValue(valueFactory.emptyPathValue());
        assertOnlyDeserializesValue(valueFactory.filledPathValue());
    }

    @Test
    @SuppressWarnings("ExtractMethodRecommender")
    void shouldGiveHelpfulErrorOnMalformedNodeStruct() throws Throwable {
        // Given
        var output = new ChunkAwareByteBufOutput();
        var buf = Unpooled.buffer();
        output.start(buf);
        var packer = new PackStream.Packer(output);

        packer.packStructHeader(1, RecordMessage.SIGNATURE);
        packer.packListHeader(1);
        packer.packStructHeader(0, CommonValueUnpacker.NODE);

        output.stop();
        BoltProtocolUtil.writeMessageBoundary(buf);

        var channel = newEmbeddedChannel();
        var dispatcher = messageDispatcher(channel);
        var memorizingDispatcher = ((MemorizingInboundMessageDispatcher) dispatcher);
        var errorFuture = new CompletableFuture<Void>();
        memorizingDispatcher.enqueue(new ResponseHandler() {
            @Override
            public void onSuccess(Map<String, Value> metadata) {
                errorFuture.complete(null);
            }

            @Override
            public void onFailure(Throwable error) {
                errorFuture.completeExceptionally(error);
            }

            @Override
            public void onRecord(Value[] fields) {
                // ignored
            }
        });
        channel.writeInbound(buf);

        // Expect
        Throwable error = assertThrows(CompletionException.class, errorFuture::join);
        error = assertInstanceOf(BoltClientException.class, error.getCause());
        assertTrue(
                error.getMessage()
                        .startsWith(
                                "Invalid message received, serialized NODE structures should have 3 fields, received NODE structure has 0 fields."));
    }

    private void assertSerializesValue(Value value) {
        assertSerializes(new RecordMessage(new Value[] {value}));
    }

    private void assertSerializes(Message message) {
        var channel = newEmbeddedChannel(new KnowledgeableMessageFormat(false));

        var packed = pack(message, channel);
        var unpackedMessage = unpack(packed, channel);

        assertEquals(message, unpackedMessage);
    }

    private EmbeddedChannel newEmbeddedChannel() {
        return newEmbeddedChannel(format);
    }

    private EmbeddedChannel newEmbeddedChannel(MessageFormat format) {
        var channel = new EmbeddedChannel();
        setMessageDispatcher(channel, new MemorizingInboundMessageDispatcher(channel, NoopLoggingProvider.INSTANCE));
        new ChannelPipelineBuilderImpl().build(format, channel.pipeline(), NoopLoggingProvider.INSTANCE, valueFactory);
        return channel;
    }

    private ByteBuf pack(Message message, EmbeddedChannel channel) {
        assertTrue(channel.writeOutbound(message));

        var packedMessages =
                channel.outboundMessages().stream().map(msg -> (ByteBuf) msg).toArray(ByteBuf[]::new);

        return Unpooled.wrappedBuffer(packedMessages);
    }

    private Message unpack(ByteBuf packed, EmbeddedChannel channel) {
        channel.writeInbound(packed);

        var dispatcher = messageDispatcher(channel);
        var memorizingDispatcher = ((MemorizingInboundMessageDispatcher) dispatcher);

        var unpackedMessages = memorizingDispatcher.messages();

        assertEquals(1, unpackedMessages.size());
        return unpackedMessages.get(0);
    }

    private void assertOnlyDeserializesValue(Value value) {
        var message = new RecordMessage(new Value[] {value});
        var packed = knowledgeablePack(message);

        var channel = newEmbeddedChannel();
        var unpackedMessage = unpack(packed, channel);

        assertEquals(message, unpackedMessage);
    }

    private ByteBuf knowledgeablePack(Message message) {
        var channel = newEmbeddedChannel(new KnowledgeableMessageFormat(false));
        assertTrue(channel.writeOutbound(message));

        var packedMessages =
                channel.outboundMessages().stream().map(msg -> (ByteBuf) msg).toArray(ByteBuf[]::new);

        return Unpooled.wrappedBuffer(packedMessages);
    }
}
