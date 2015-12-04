/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.v1.internal;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.driver.v1.Function;
import org.neo4j.driver.v1.Value;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.v1.Values.NULL;
import static org.neo4j.driver.v1.Values.value;

public class SimpleNodeTest
{
    @Test
    public void extractValuesFromNode()
    {
        // GIVEN
        SimpleNode node = createNode();
        Function<Value,Integer> extractor = new Function<Value,Integer>()
        {
            @Override
            public Integer apply( Value value )
            {
                return value.asInt();
            }
        };

        //WHEN
        Iterable<Integer> values = node.values( extractor );

        //THEN
        Iterator<Integer> iterator = values.iterator();
        assertThat( iterator.next(), equalTo( 1 ) );
        assertThat( iterator.next(), equalTo( 2 ) );
        assertFalse( iterator.hasNext() );
    }

    @Test
    public void accessUnknownKeyShouldBeNull()
    {
        SimpleNode node = createNode();

        assertThat( node.value( "k1" ), equalTo( value( 1 ) ) );
        assertThat( node.value( "k2" ), equalTo( value( 2 ) ) );
        assertThat( node.value( "k3" ), equalTo( NULL ) );
    }

    private SimpleNode createNode()
    {
        Map<String,Value> props = new HashMap<>();
        props.put( "k1", value( 1 ) );
        props.put( "k2", value( 2 ) );
        return new SimpleNode( 42L, Collections.singletonList( "L" ), props );
    }

}
