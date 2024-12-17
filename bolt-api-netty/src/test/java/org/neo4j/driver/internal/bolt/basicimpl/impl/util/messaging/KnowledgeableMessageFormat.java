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
package org.neo4j.driver.internal.bolt.basicimpl.impl.util.messaging;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.neo4j.bolt.api.test.values.TestNode;
import org.neo4j.bolt.api.test.values.TestPath;
import org.neo4j.bolt.api.test.values.TestRelationship;
import org.neo4j.bolt.api.test.values.TestValue;
import org.neo4j.bolt.api.test.values.TestValueFactory;
import org.neo4j.bolt.api.test.values.impl.Entity;
import org.neo4j.driver.internal.bolt.api.values.Value;
import org.neo4j.driver.internal.bolt.api.values.ValueFactory;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.AbstractMessageWriter;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.MessageEncoder;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.common.CommonValuePacker;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.common.CommonValueUnpacker;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.encode.DiscardAllMessageEncoder;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.encode.PullAllMessageEncoder;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.encode.ResetMessageEncoder;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.request.DiscardAllMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.request.PullAllMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.request.ResetMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.response.FailureMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.response.IgnoredMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.response.RecordMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.response.SuccessMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v3.MessageFormatV3;
import org.neo4j.driver.internal.bolt.basicimpl.impl.packstream.PackOutput;

/**
 * This class provides the missing server side packing methods to serialize Node, Relationship and Path. It also allows writing of server side messages like
 * SUCCESS, FAILURE, IGNORED and RECORD.
 */
public class KnowledgeableMessageFormat extends MessageFormatV3 {
    private final boolean elementIdEnabled;
    private boolean dateTimeUtcEnabled;

    public KnowledgeableMessageFormat(boolean elementIdEnabled) {
        this.elementIdEnabled = elementIdEnabled;
    }

    @Override
    public Writer newWriter(PackOutput output, ValueFactory valueFactory) {
        return new KnowledgeableMessageWriter(
                output, elementIdEnabled, dateTimeUtcEnabled, (TestValueFactory) valueFactory);
    }

    @Override
    public void enableDateTimeUtc() {
        dateTimeUtcEnabled = true;
    }

    private static class KnowledgeableMessageWriter extends AbstractMessageWriter {
        KnowledgeableMessageWriter(
                PackOutput output, boolean enableElementId, boolean dateTimeUtcEnabled, TestValueFactory valueFactory) {
            super(
                    new KnowledgeableValuePacker(output, enableElementId, dateTimeUtcEnabled),
                    buildEncoders(),
                    valueFactory);
        }

        static Map<Byte, MessageEncoder> buildEncoders() {
            Map<Byte, MessageEncoder> result = new HashMap<>(10);
            // request message encoders
            result.put(DiscardAllMessage.SIGNATURE, new DiscardAllMessageEncoder());
            result.put(PullAllMessage.SIGNATURE, new PullAllMessageEncoder());
            result.put(ResetMessage.SIGNATURE, new ResetMessageEncoder());
            // response message encoders
            result.put(FailureMessage.SIGNATURE, new FailureMessageEncoder());
            result.put(IgnoredMessage.SIGNATURE, new IgnoredMessageEncoder());
            result.put(RecordMessage.SIGNATURE, new RecordMessageEncoder());
            result.put(SuccessMessage.SIGNATURE, new SuccessMessageEncoder());
            return result;
        }
    }

    private static class KnowledgeableValuePacker extends CommonValuePacker {
        private final boolean elementIdEnabled;

        KnowledgeableValuePacker(PackOutput output, boolean elementIdEnabled, boolean dateTimeUtcEnabled) {
            super(output, dateTimeUtcEnabled);
            this.elementIdEnabled = elementIdEnabled;
        }

        @Override
        protected void packInternalValue(Value value) throws IOException {
            switch (value.type()) {
                case NODE -> {
                    var node = ((TestValue) value).asNode();
                    packNode(node);
                }
                case RELATIONSHIP -> {
                    var rel = ((TestValue) value).asRelationship();
                    packRelationship(rel);
                }
                case PATH -> {
                    var path = ((TestValue) value).asPath();
                    packPath(path);
                }
                default -> super.packInternalValue(value);
            }
        }

        private void packPath(TestPath path) throws IOException {
            packer.packStructHeader(3, CommonValueUnpacker.PATH);

            // Unique nodes
            Map<TestNode, Integer> nodeIdx = new LinkedHashMap<>(path.length() + 1);
            for (var node : path.nodes()) {
                if (!nodeIdx.containsKey(node)) {
                    nodeIdx.put(node, nodeIdx.size());
                }
            }
            packer.packListHeader(nodeIdx.size());
            for (var node : nodeIdx.keySet()) {
                packNode(node);
            }

            // Unique rels
            Map<TestRelationship, Integer> relIdx = new LinkedHashMap<>(path.length());
            for (var rel : path.relationships()) {
                if (!relIdx.containsKey(rel)) {
                    relIdx.put(rel, relIdx.size() + 1);
                }
            }
            packer.packListHeader(relIdx.size());
            for (var rel : relIdx.keySet()) {
                packer.packStructHeader(elementIdEnabled ? 4 : 3, CommonValueUnpacker.UNBOUND_RELATIONSHIP);
                packer.pack(rel.id());
                packer.pack(rel.typeString());
                packProperties(rel);
                if (elementIdEnabled) {
                    packer.pack(rel.elementId());
                }
            }

            // Sequence
            packer.packListHeader(path.length() * 2);
            for (var seg : path) {
                var rel = seg.relationship();
                var relEndId = rel.endNodeId();
                var segEndId = seg.end().id();
                var size = relEndId == segEndId ? relIdx.get(rel) : -relIdx.get(rel);
                packer.pack(size);
                packer.pack(nodeIdx.get(seg.end()));
            }
        }

        private void packRelationship(TestRelationship rel) throws IOException {
            packer.packStructHeader(elementIdEnabled ? 8 : 5, CommonValueUnpacker.RELATIONSHIP);
            packer.pack(rel.id());
            packer.pack(rel.startNodeId());
            packer.pack(rel.endNodeId());

            packer.pack(rel.typeString());

            packProperties(rel);

            if (elementIdEnabled) {
                packer.pack(rel.elementId());
                packer.pack(rel.startNodeElementId());
                packer.pack(rel.endNodeElementId());
            }
        }

        private void packNode(TestNode node) throws IOException {
            packer.packStructHeader(elementIdEnabled ? 4 : 3, CommonValueUnpacker.NODE);
            packer.pack(node.id());

            var labels = node.labels();
            packer.packListHeader(
                    (int) StreamSupport.stream(labels.spliterator(), false).count());
            for (var label : labels) {
                packer.pack(label);
            }

            packProperties(node);

            if (elementIdEnabled) {
                packer.pack(node.elementId());
            }
        }

        private void packProperties(Entity entity) throws IOException {
            var keys = entity.keys();
            packer.packMapHeader(entity.size());
            for (var propKey : keys) {
                packer.pack(propKey);
                packInternalValue(entity.get(propKey));
            }
        }
    }
}
