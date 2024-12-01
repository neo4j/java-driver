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
package org.neo4j.bolt.api.test.values;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

import java.time.DateTimeException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.api.test.values.impl.InternalNode;
import org.neo4j.bolt.api.test.values.impl.InternalPath;
import org.neo4j.bolt.api.test.values.impl.InternalRelationship;
import org.neo4j.bolt.api.test.values.impl.NodeValue;
import org.neo4j.bolt.api.test.values.impl.PathValue;
import org.neo4j.bolt.api.test.values.impl.RelationshipValue;
import org.neo4j.bolt.api.test.values.impl.UnsupportedDateTimeValue;
import org.neo4j.driver.internal.bolt.api.values.Node;
import org.neo4j.driver.internal.bolt.api.values.Path;
import org.neo4j.driver.internal.bolt.api.values.Relationship;
import org.neo4j.driver.internal.bolt.api.values.Segment;
import org.neo4j.driver.internal.bolt.api.values.Value;
import org.neo4j.driver.internal.bolt.api.values.ValueFactory;

public class TestValueFactory implements ValueFactory {
    public static final TestValueFactory INSTANCE = new TestValueFactory();

    private TestValueFactory() {}

    @Override
    public Value value(Object value) {
        return Values.value(value);
    }

    @Override
    public Node node(long id, String elementId, Collection<String> labels, Map<String, Value> properties) {
        return new InternalNode(id, elementId, labels, properties);
    }

    @Override
    public Relationship relationship(
            long id,
            String elementId,
            long start,
            String startElementId,
            long end,
            String endElementId,
            String type,
            Map<String, Value> properties) {
        return new InternalRelationship(id, elementId, start, startElementId, end, endElementId, type, properties);
    }

    @Override
    public Segment segment(Node start, Relationship relationship, Node end) {
        return new InternalPath.SelfContainedSegment((TestNode) start, (TestRelationship) relationship, (TestNode) end);
    }

    @Override
    public Path path(List<Segment> segments, List<Node> nodes, List<Relationship> relationships) {
        return new InternalPath(
                segments.stream().map(TestPath.TestSegment.class::cast).toList(),
                nodes.stream().map(TestNode.class::cast).toList(),
                relationships.stream().map(TestRelationship.class::cast).toList());
    }

    @Override
    public Value isoDuration(long months, long days, long seconds, int nanoseconds) {
        return Values.isoDuration(months, days, seconds, nanoseconds);
    }

    @Override
    public Value point(int srid, double x, double y) {
        return Values.point(srid, x, y);
    }

    @Override
    public Value point(int srid, double x, double y, double z) {
        return Values.point(srid, x, y, z);
    }

    @Override
    public Value unsupportedDateTimeValue(DateTimeException e) {
        return new UnsupportedDateTimeValue(e);
    }

    public Value emptyNodeValue() {
        return new NodeValue(new InternalNode(1234, singletonList("User"), new HashMap<>()));
    }

    public Value filledNodeValue() {
        return new NodeValue(new InternalNode(1234, singletonList("User"), singletonMap("name", value("Dodo"))));
    }

    public Value emptyRelationshipValue() {
        return new RelationshipValue(new InternalRelationship(1234, 1, 2, "KNOWS"));
    }

    public Value filledRelationshipValue() {
        return new RelationshipValue(
                new InternalRelationship(1234, 1, 2, "KNOWS", singletonMap("name", value("Dodo"))));
    }

    public Value filledPathValue() {
        return new PathValue(new InternalPath(
                new InternalNode(42L), new InternalRelationship(43L, 42L, 44L, "T"), new InternalNode(44L)));
    }

    public Value emptyPathValue() {
        return new PathValue(new InternalPath(new InternalNode(1)));
    }
}
