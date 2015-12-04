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
package org.neo4j.driver.v1.internal.value;

import org.neo4j.driver.v1.Node;
import org.neo4j.driver.v1.Type;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.internal.types.StandardTypeSystem;

public class NodeValue extends ValueAdapter
{
    private final Node adapted;

    public NodeValue( Node adapted )
    {
        this.adapted = adapted;
    }

    @Override
    public Object asObject()
    {
        return asNode();
    }

    @Override
    public Node asNode()
    {
        return adapted;
    }

    private boolean isNode()
    {
        return true;
    }

    @Override
    public int size()
    {
        int count = 0;
        for ( String ignore : adapted.keys() ) { count++; }
        return count;
    }

    @Override
    public Iterable<String> keys()
    {
        return adapted.keys();
    }

    @Override
    public Value value( String key )
    {
        return adapted.value( key );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        NodeValue values = (NodeValue) o;
        return adapted == values.adapted || adapted.equals( values.adapted );

    }

    @Override
    public Type type()
    {
        return StandardTypeSystem.TYPE_SYSTEM.NODE();
    }

    @Override
    public int hashCode()
    {
        return adapted != null ? adapted.hashCode() : 0;
    }

    @Override
    public String valueAsString()
    {
        return adapted.toString();
    }
}
