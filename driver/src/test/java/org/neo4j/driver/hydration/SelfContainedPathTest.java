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
package org.neo4j.driver.hydration;

import java.util.Arrays;
import java.util.List;

import org.hamcrest.MatcherAssert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.driver.v1.types.*;
import org.neo4j.driver.v1.util.Lists;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class SelfContainedPathTest
{
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    // (A)-[AB:KNOWS]->(B)<-[CB:KNOWS]-(C)-[CD:KNOWS]->(D)
    private SelfContainedPath testPath()
    {
        return new SelfContainedPath(
                new SelfContainedNode( 1 ),
                new SelfContainedRelationship( -1, 1, 2, "KNOWS" ),
                new SelfContainedNode( 2 ),
                new SelfContainedRelationship( -2, 3, 2, "KNOWS" ),
                new SelfContainedNode( 3 ),
                new SelfContainedRelationship( -3, 3, 4, "KNOWS" ),
                new SelfContainedNode( 4 )
        );
    }

    @Test
    public void pathSizeShouldReturnNumberOfRelationships()
    {
        // When
        SelfContainedPath path = testPath();

        // Then
        assertThat( path.length(), equalTo( 3 ) );
    }

    @Test
    public void shouldBeAbleToCreatePathWithSingleNode()
    {
        // When
        SelfContainedPath path = new SelfContainedPath( new SelfContainedNode( 1 ) );

        // Then
        assertThat( path.length(), equalTo( 0 ) );
    }

    @Test
    public void shouldBeAbleToIterateOverPathAsSegments() throws Exception
    {
        // Given
        SelfContainedPath path = testPath();

        // When
        List<Path.Segment> segments = Lists.asList( path );

        // Then
        MatcherAssert.assertThat( segments, equalTo( Arrays.asList( (Path.Segment)
                                new SelfContainedPath.Segment(
                                        new SelfContainedNode( 1 ),
                                        new SelfContainedRelationship( -1, 1, 2, "KNOWS" ),
                                        new SelfContainedNode( 2 )
                                ),
                        new SelfContainedPath.Segment(
                                new SelfContainedNode( 2 ),
                                new SelfContainedRelationship( -2, 3, 2, "KNOWS" ),
                                new SelfContainedNode( 3 )
                        ),
                        new SelfContainedPath.Segment(
                                new SelfContainedNode( 3 ),
                                new SelfContainedRelationship( -3, 3, 4, "KNOWS" ),
                                new SelfContainedNode( 4 )
                        )
                )
        ) );
    }

    @Test
    public void shouldBeAbleToIterateOverPathNodes() throws Exception
    {
        // Given
        SelfContainedPath path = testPath();

        // When
        List<Node> segments = Lists.asList( path.nodes() );

        // Then
        assertThat( segments, equalTo( Arrays.asList( (Node)
                new SelfContainedNode( 1 ),
                new SelfContainedNode( 2 ),
                new SelfContainedNode( 3 ),
                new SelfContainedNode( 4 ) ) ) );
    }

    @Test
    public void shouldBeAbleToIterateOverPathRelationships() throws Exception
    {
        // Given
        SelfContainedPath path = testPath();

        // When
        List<Relationship> segments = Lists.asList( path.relationships() );

        // Then
        assertThat( segments, equalTo( Arrays.asList( (Relationship)
                new SelfContainedRelationship( -1, 1, 2, "KNOWS" ),
                new SelfContainedRelationship( -2, 3, 2, "KNOWS" ),
                new SelfContainedRelationship( -3, 3, 4, "KNOWS" ) ) ) );
    }

    @Test
    public void shouldNotBeAbleToCreatePathWithNoEntities()
    {
        // Expect
        thrown.expect( IllegalArgumentException.class );

        // When
        new SelfContainedPath();

    }

    @Test
    public void shouldNotBeAbleToCreatePathWithEvenNumberOfEntities()
    {
        // Expect
        thrown.expect( IllegalArgumentException.class );

        // When
        new SelfContainedPath(
                new SelfContainedNode( 1 ),
                new SelfContainedRelationship( 2, 3, 4, "KNOWS" ) );

    }

    @Test
    public void shouldNotBeAbleToCreatePathWithNullEntities()
    {
        // Expect
        thrown.expect( IllegalArgumentException.class );

        // When
        SelfContainedNode nullNode = null;
        //noinspection ConstantConditions
        new SelfContainedPath( nullNode );

    }

    @Test
    public void shouldNotBeAbleToCreatePathWithNodeThatDoesNotConnect()
    {
        // Expect
        thrown.expect( IllegalArgumentException.class );

        // When
        new SelfContainedPath(
                new SelfContainedNode( 1 ),
                new SelfContainedRelationship( 2, 1, 3, "KNOWS" ),
                new SelfContainedNode( 4 ) );

    }

    @Test
    public void shouldNotBeAbleToCreatePathWithRelationshipThatDoesNotConnect()
    {
        // Expect
        thrown.expect( IllegalArgumentException.class );

        // When
        new SelfContainedPath(
                new SelfContainedNode( 1 ),
                new SelfContainedRelationship( 2, 3, 4, "KNOWS" ),
                new SelfContainedNode( 3 ) );

    }

}
