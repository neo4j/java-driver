/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

package org.neo4j.driver.v1.types;

import org.neo4j.driver.v1.Value;
import org.neo4j.driver.hydration.Hydrant;
import org.neo4j.driver.hydration.HydrationException;
import org.neo4j.driver.packstream.StructureHeader;
import org.neo4j.driver.packstream.UnpackStream;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class GraphHydrant extends Hydrant
{
    public static final byte NODE = 'N';
    public static final byte RELATIONSHIP = 'R';
    public static final byte RELATIONSHIP_DETAIL = 'r';
    public static final byte PATH = 'P';

    public GraphHydrant(UnpackStream unpackStream)
    {
        super(unpackStream);
    }

    protected Node hydrateNode(long size) throws IOException, HydrationException
    {
        checkSize(size, 3);
        long id = unpackLong();
        List<String> labels = unpackStringList();
        Map<String, Value> properties = unpackMap();
        return new SelfContainedNode(id, labels, properties);
    }

    protected Relationship hydrateRelationship(long size) throws IOException, HydrationException
    {
        checkSize(size, 5);
        long id = unpackLong();
        long startNodeId = unpackLong();
        long endNodeId = unpackLong();
        String type = unpackString();
        Map<String, Value> properties = unpackMap();
        return new SelfContainedRelationship(id, startNodeId, endNodeId, type, properties);
    }

    private RelationshipDetail hydrateRelationshipDetail(long size) throws IOException, HydrationException
    {
        checkSize(size, 3);

        long id = unpackLong();
        String type = unpackString();
        Map<String, Value> properties = unpackMap();
        return new RelationshipDetail(id, type, properties);
    }

    protected Path hydratePath(long size) throws IOException, HydrationException
    {
        checkSize(size, 3);

        // List of unique nodes
        Node[] uniqueNodes = new Node[unpackListHeader()];
        for (int i = 0; i < uniqueNodes.length; i++)
        {
            StructureHeader header = unpackStructureHeader();
            checkSignature(header.signature(), NODE);
            uniqueNodes[i] = hydrateNode(header.size());
        }

        // List of unique relationships, without start/end information
        RelationshipDetail[] uniqueRelationshipDetails = new RelationshipDetail[unpackListHeader()];
        for (int i = 0; i < uniqueRelationshipDetails.length; i++)
        {
            StructureHeader header = unpackStructureHeader();
            checkSignature(header.signature(), RELATIONSHIP_DETAIL);
            uniqueRelationshipDetails[i] = hydrateRelationshipDetail(header.size());
        }

        // Path sequence
        int length = unpackListHeader();

        // Knowing the sequence length, we can create the arrays that will represent the nodes, rels and segments in their "path order"
        Path.Segment[] segments = new Path.Segment[length / 2];
        Node[] nodes = new Node[segments.length + 1];
        Relationship[] relationships = new Relationship[segments.length];

        Node lastNode = uniqueNodes[0], nextNode; // Start node is always 0, and isn't encoded in the sequence
        nodes[0] = lastNode;
        Relationship relationship;
        for (int i = 0; i < segments.length; i++)
        {
            int relationshipIndex = (int) unpackLong();
            nextNode = uniqueNodes[(int) unpackLong()];
            // Negative rel index means this rel was traversed "inversed" from its direction
            if (relationshipIndex < 0)
            {
                RelationshipDetail rel = uniqueRelationshipDetails[(-relationshipIndex) - 1];
                relationship = new SelfContainedRelationship(rel.id(), nextNode.id(), lastNode.id(), rel.type(), rel.properties());
            }
            else
            {
                RelationshipDetail rel = uniqueRelationshipDetails[relationshipIndex - 1];
                relationship = new SelfContainedRelationship(rel.id(), lastNode.id(), nextNode.id(), rel.type(), rel.properties());
            }
            nodes[i + 1] = nextNode;
            relationships[i] = relationship;
            segments[i] = new SelfContainedPath.Segment(lastNode, relationship, nextNode);
            lastNode = nextNode;
        }
        return new SelfContainedPath(Arrays.asList(segments), Arrays.asList(nodes), Arrays.asList(relationships));
    }

    public Value hydrateStructure(StructureHeader structureHeader) throws IOException, HydrationException
    {
        switch (structureHeader.signature())
        {
        case NODE:
            return new NodeValue(hydrateNode(structureHeader.size()));
        case RELATIONSHIP:
            return new RelationshipValue(hydrateRelationship(structureHeader.size()));
        case PATH:
            return new PathValue(hydratePath(structureHeader.size()));
        default:
            return super.hydrateStructure(structureHeader);
        }
    }

}
