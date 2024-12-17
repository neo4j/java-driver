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
package org.neo4j.bolt.api.test.values.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.neo4j.bolt.api.test.values.TestNode;
import org.neo4j.bolt.api.test.values.TestPath;
import org.neo4j.bolt.api.test.values.TestRelationship;
import org.neo4j.driver.internal.bolt.api.values.Value;

public class InternalPath implements TestPath, AsValue {
    public record SelfContainedSegment(TestNode start, TestRelationship relationship, TestNode end)
            implements TestSegment {

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            var that = (SelfContainedSegment) other;
            return start.equals(that.start) && end.equals(that.end) && relationship.equals(that.relationship);
        }

        @Override
        public String toString() {
            return String.format(
                    relationship.startNodeId() == start.id() ? "(%s)-[%s:%s]->(%s)" : "(%s)<-[%s:%s]-(%s)",
                    start.id(),
                    relationship.id(),
                    relationship.typeString(),
                    end.id());
        }
    }

    private static boolean isEndpoint(TestNode node, TestRelationship relationship) {
        return node.id() == relationship.startNodeId() || node.id() == relationship.endNodeId();
    }

    private final List<TestNode> nodes;
    private final List<TestRelationship> relationships;
    private final List<TestSegment> segments;

    public InternalPath(List<Entity> alternatingNodeAndRel) {
        nodes = newList(alternatingNodeAndRel.size() / 2 + 1);
        relationships = newList(alternatingNodeAndRel.size() / 2);
        segments = newList(alternatingNodeAndRel.size() / 2);

        if (alternatingNodeAndRel.size() % 2 == 0) {
            throw new IllegalArgumentException("An odd number of entities are required to build a path");
        }
        TestNode lastNode = null;
        TestRelationship lastRelationship = null;
        var index = 0;
        for (var entity : alternatingNodeAndRel) {
            if (entity == null) {
                throw new IllegalArgumentException("Path entities cannot be null");
            }
            if (index % 2 == 0) {
                // even index - this should be a node
                try {
                    lastNode = (TestNode) entity;
                    if (nodes.isEmpty() || (lastRelationship != null && isEndpoint(lastNode, lastRelationship))) {
                        nodes.add(lastNode);
                    } else {
                        throw new IllegalArgumentException("Node argument " + index
                                + " is not an endpoint of relationship argument " + (index - 1));
                    }
                } catch (ClassCastException e) {
                    var cls = entity.getClass().getName();
                    throw new IllegalArgumentException("Expected argument " + index + " to be a node " + index
                            + " but found a " + cls + " " + "instead");
                }
            } else {
                // odd index - this should be a relationship
                try {
                    lastRelationship = (TestRelationship) entity;
                    if (isEndpoint(lastNode, lastRelationship)) {
                        relationships.add(lastRelationship);
                    } else {
                        throw new IllegalArgumentException("Node argument " + (index - 1)
                                + " is not an endpoint of relationship argument " + index);
                    }
                } catch (ClassCastException e) {
                    var cls = entity.getClass().getName();
                    throw new IllegalArgumentException(
                            "Expected argument " + index + " to be a relationship but found a " + cls + " instead");
                }
            }
            index += 1;
        }
        buildSegments();
    }

    public InternalPath(Entity... alternatingNodeAndRel) {
        this(Arrays.asList(alternatingNodeAndRel));
    }

    public InternalPath(List<TestSegment> segments, List<TestNode> nodes, List<TestRelationship> relationships) {
        this.segments = segments;
        this.nodes = nodes;
        this.relationships = relationships;
    }

    private <T> List<T> newList(int size) {
        return size == 0 ? Collections.emptyList() : new ArrayList<>(size);
    }

    @Override
    public int length() {
        return relationships.size();
    }

    @Override
    public boolean contains(TestNode node) {
        return nodes.contains(node);
    }

    @Override
    public boolean contains(TestRelationship relationship) {
        return relationships.contains(relationship);
    }

    @Override
    public Iterable<TestNode> nodes() {
        return nodes;
    }

    @Override
    public Iterable<TestRelationship> relationships() {
        return relationships;
    }

    @Override
    public TestNode start() {
        return nodes.get(0);
    }

    @Override
    public TestNode end() {
        return nodes.get(nodes.size() - 1);
    }

    @Override
    public Iterator<TestSegment> iterator() {
        return segments.iterator();
    }

    @Override
    public Value asValue() {
        return new PathValue(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        var segments1 = (InternalPath) o;

        return segments.equals(segments1.segments);
    }

    @Override
    public int hashCode() {
        return segments.hashCode();
    }

    @Override
    public String toString() {

        return "path" + segments;
    }

    private void buildSegments() {
        for (var i = 0; i < relationships.size(); i++) {
            segments.add(new SelfContainedSegment(nodes.get(i), relationships.get(i), nodes.get(i + 1)));
        }
    }
}
