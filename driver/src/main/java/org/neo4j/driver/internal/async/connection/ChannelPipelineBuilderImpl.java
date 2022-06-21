/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
package org.neo4j.driver.internal.async.connection;

import static org.neo4j.driver.internal.async.connection.ChannelAttributes.addBoltPatchesListener;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import org.neo4j.driver.Logging;
import org.neo4j.driver.internal.async.inbound.ChannelErrorHandler;
import org.neo4j.driver.internal.async.inbound.ChunkDecoder;
import org.neo4j.driver.internal.async.inbound.InboundMessageHandler;
import org.neo4j.driver.internal.async.inbound.MessageDecoder;
import org.neo4j.driver.internal.async.outbound.OutboundMessageHandler;
import org.neo4j.driver.internal.messaging.MessageFormat;

public class ChannelPipelineBuilderImpl implements ChannelPipelineBuilder {
    @Override
    public void build(MessageFormat messageFormat, ChannelPipeline pipeline, Logging logging) {
        // inbound handlers
        pipeline.addLast(new ChunkDecoder(logging));
        pipeline.addLast(new MessageDecoder());
        Channel channel = pipeline.channel();
        InboundMessageHandler inboundMessageHandler = new InboundMessageHandler(messageFormat, logging);
        addBoltPatchesListener(channel, inboundMessageHandler);
        pipeline.addLast(inboundMessageHandler);

        // outbound handlers
        OutboundMessageHandler outboundMessageHandler = new OutboundMessageHandler(messageFormat, logging);
        addBoltPatchesListener(channel, outboundMessageHandler);
        pipeline.addLast(OutboundMessageHandler.NAME, outboundMessageHandler);

        // last one - error handler
        pipeline.addLast(new ChannelErrorHandler(logging));
    }
}
