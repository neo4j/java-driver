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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.bolt.api.values.ValueFactory;
import org.neo4j.driver.internal.bolt.basicimpl.impl.NoopLoggingProvider;
import org.neo4j.driver.internal.bolt.basicimpl.impl.async.inbound.ChannelErrorHandler;
import org.neo4j.driver.internal.bolt.basicimpl.impl.async.inbound.ChunkDecoder;
import org.neo4j.driver.internal.bolt.basicimpl.impl.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.bolt.basicimpl.impl.async.inbound.InboundMessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.impl.async.inbound.MessageDecoder;
import org.neo4j.driver.internal.bolt.basicimpl.impl.async.outbound.OutboundMessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v3.MessageFormatV3;

class ChannelPipelineBuilderImplTest {
    @Test
    void shouldBuildPipeline() {
        var channel = new EmbeddedChannel();
        ChannelAttributes.setMessageDispatcher(
                channel, new InboundMessageDispatcher(channel, NoopLoggingProvider.INSTANCE));

        new ChannelPipelineBuilderImpl()
                .build(
                        new MessageFormatV3(),
                        channel.pipeline(),
                        NoopLoggingProvider.INSTANCE,
                        mock(ValueFactory.class));

        var iterator = channel.pipeline().iterator();
        assertInstanceOf(ChunkDecoder.class, iterator.next().getValue());
        assertInstanceOf(MessageDecoder.class, iterator.next().getValue());
        assertInstanceOf(InboundMessageHandler.class, iterator.next().getValue());

        assertInstanceOf(OutboundMessageHandler.class, iterator.next().getValue());

        assertInstanceOf(ChannelErrorHandler.class, iterator.next().getValue());

        assertFalse(iterator.hasNext());
    }
}
