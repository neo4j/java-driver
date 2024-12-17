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
package org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.driver.internal.bolt.api.values.Node;
import org.neo4j.driver.internal.bolt.api.values.Path;
import org.neo4j.driver.internal.bolt.api.values.Relationship;
import org.neo4j.driver.internal.bolt.api.values.Segment;
import org.neo4j.driver.internal.bolt.api.values.Type;
import org.neo4j.driver.internal.bolt.api.values.Value;
import org.neo4j.driver.internal.bolt.api.values.ValueFactory;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.common.CommonValueUnpacker;
import org.neo4j.driver.internal.bolt.basicimpl.impl.packstream.PackInput;

public class ValueUnpackerV5 extends CommonValueUnpacker {
    private static final int NODE_FIELDS = 4;
    private static final int RELATIONSHIP_FIELDS = 8;

    public ValueUnpackerV5(PackInput input, ValueFactory valueFactory) {
        super(input, true, valueFactory);
    }

    @Override
    protected int getNodeFields() {
        return NODE_FIELDS;
    }

    @Override
    protected int getRelationshipFields() {
        return RELATIONSHIP_FIELDS;
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    protected Node unpackNode() throws IOException {
        var urn = unpacker.unpackLong();

        var numLabels = (int) unpacker.unpackListHeader();
        List<String> labels = new ArrayList<>(numLabels);
        for (var i = 0; i < numLabels; i++) {
            labels.add(unpacker.unpackString());
        }
        var numProps = (int) unpacker.unpackMapHeader();
        Map<String, Value> props = new HashMap<>(numProps);
        for (var j = 0; j < numProps; j++) {
            var key = unpacker.unpackString();
            props.put(key, unpack());
        }

        var elementId = unpacker.unpackString();

        return valueFactory.node(urn, elementId, labels, props);
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    protected Path unpackPath() throws IOException {
        // List of unique nodes
        var uniqNodes = new Node[(int) unpacker.unpackListHeader()];
        for (var i = 0; i < uniqNodes.length; i++) {
            ensureCorrectStructSize(Type.NODE, getNodeFields(), unpacker.unpackStructHeader());
            ensureCorrectStructSignature("NODE", NODE, unpacker.unpackStructSignature());
            uniqNodes[i] = unpackNode();
        }

        // List of unique relationships, without start/end information
        var uniqRels = new Relationship[(int) unpacker.unpackListHeader()];
        for (var i = 0; i < uniqRels.length; i++) {
            ensureCorrectStructSize(Type.RELATIONSHIP, 4, unpacker.unpackStructHeader());
            ensureCorrectStructSignature(
                    "UNBOUND_RELATIONSHIP", UNBOUND_RELATIONSHIP, unpacker.unpackStructSignature());
            var id = unpacker.unpackLong();
            var relType = unpacker.unpackString();
            var props = unpackMap();
            var elementId = unpacker.unpackString();
            uniqRels[i] = valueFactory.relationship(
                    id, elementId, -1, String.valueOf(-1), -1, String.valueOf(-1), relType, props);
        }

        // Path sequence
        var length = (int) unpacker.unpackListHeader();

        // Knowing the sequence length, we can create the arrays that will represent the nodes, rels and segments in
        // their "path order"
        var segments = new Segment[length / 2];
        var nodes = new Node[segments.length + 1];
        var rels = new Relationship[segments.length];

        Node prevNode = uniqNodes[0], nextNode; // Start node is always 0, and isn't encoded in the sequence
        nodes[0] = prevNode;
        Relationship rel;
        for (var i = 0; i < segments.length; i++) {
            var relIdx = (int) unpacker.unpackLong();
            nextNode = uniqNodes[(int) unpacker.unpackLong()];
            // Negative rel index means this rel was traversed "inversed" from its direction
            if (relIdx < 0) {
                rel = uniqRels[(-relIdx) - 1]; // -1 because rel idx are 1-indexed
                setStartAndEnd(rel, nextNode, prevNode);
            } else {
                rel = uniqRels[relIdx - 1];
                setStartAndEnd(rel, prevNode, nextNode);
            }

            nodes[i + 1] = nextNode;
            rels[i] = rel;
            segments[i] = valueFactory.segment(prevNode, rel, nextNode);
            prevNode = nextNode;
        }
        return valueFactory.path(Arrays.asList(segments), Arrays.asList(nodes), Arrays.asList(rels));
    }

    private void setStartAndEnd(Relationship rel, Node start, Node end) {
        rel.setStartAndEnd(start.id(), start.elementId(), end.id(), end.elementId());
    }

    @Override
    protected Relationship unpackRelationship() throws IOException {
        var urn = unpacker.unpackLong();
        var startUrn = unpacker.unpackLong();
        var endUrn = unpacker.unpackLong();
        var relType = unpacker.unpackString();
        var props = unpackMap();
        var elementId = unpacker.unpackString();
        var startElementId = unpacker.unpackString();
        var endElementId = unpacker.unpackString();

        return valueFactory.relationship(
                urn, elementId, startUrn, startElementId, endUrn, endElementId, relType, props);
    }
}
