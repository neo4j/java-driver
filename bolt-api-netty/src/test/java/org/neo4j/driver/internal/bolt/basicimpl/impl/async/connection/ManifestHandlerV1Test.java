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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.MockitoAnnotations.openMocks;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.exception.BoltClientException;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.BoltProtocol;

class ManifestHandlerV1Test {
    ManifestHandlerV1 handler;

    EmbeddedChannel embeddedChannel = new EmbeddedChannel();

    @Mock
    LoggingProvider loggingProvider;

    System.Logger logger;

    @BeforeEach
    void beforeEach() {
        openMocks(this);
        given(loggingProvider.getLog(any(Class.class))).willReturn(mock(System.Logger.class));
        handler = new ManifestHandlerV1(embeddedChannel, loggingProvider);
    }

    @Test
    void shouldThrowOnRangeOverflow() {
        for (var val : new byte[] {-1, -1, -1, -1, -1, -1, -1, -1, -1}) {
            handler.decode(Unpooled.copiedBuffer(new byte[] {val}));
        }

        assertThrows(BoltClientException.class, () -> handler.decode(Unpooled.copiedBuffer(new byte[] {-1})));
    }

    @Test
    void shouldThrowOnRangeLargerThanMaxInteger() {
        for (var val : new byte[] {-1, -1, -1, -1}) {
            handler.decode(Unpooled.copiedBuffer(new byte[] {val}));
        }

        assertThrows(BoltClientException.class, () -> handler.decode(Unpooled.copiedBuffer(new byte[] {0b00001111})));
    }

    @ParameterizedTest
    @MethodSource("shouldSelectProtocolArgs")
    void shouldSelectProtocol(BoltProtocol protocol) {
        handler.decode(Unpooled.copiedBuffer(new byte[] {1}));
        handler.decode(Unpooled.copyInt(protocol.version().toInt()));
        assertEquals(protocol, handler.complete());
    }

    static Stream<Arguments> shouldSelectProtocolArgs() {
        return BoltProtocolUtil.versionToProtocol.values().stream().map(Arguments::of);
    }

    @Test
    void shouldThrowOnUnsupportedVersion() {
        handler.decode(Unpooled.copiedBuffer(new byte[] {1}));
        handler.decode(Unpooled.copyInt(0x00000104));

        assertThrows(BoltClientException.class, () -> handler.complete());
        var outboundMessages = embeddedChannel.outboundMessages();
        assertEquals(2, outboundMessages.size());
        assertEquals(Unpooled.copyInt(0), outboundMessages.poll());
        assertEquals(Unpooled.copiedBuffer(new byte[] {0}), outboundMessages.poll());
    }
}
