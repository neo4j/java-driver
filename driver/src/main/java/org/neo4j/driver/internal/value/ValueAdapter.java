/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.driver.internal.value;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.types.InternalMapAccessorWithDefaultValue;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.internal.types.TypeRepresentation;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.value.NotMultiValued;
import org.neo4j.driver.v1.exceptions.value.Uncoercible;
import org.neo4j.driver.v1.exceptions.value.Unsizable;
import org.neo4j.driver.v1.types.Entity;
import org.neo4j.driver.v1.types.CypherDuration;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Point;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.driver.v1.types.Type;
import org.neo4j.driver.v1.util.Function;

import static java.util.Collections.emptyList;
import static org.neo4j.driver.v1.Values.ofObject;
import static org.neo4j.driver.v1.Values.ofValue;

public abstract class ValueAdapter extends InternalMapAccessorWithDefaultValue implements InternalValue
{
    @Override
    public Value asValue()
    {
        return this;
    }

    @Override
    public boolean hasType( Type type )
    {
        return type.isTypeOf( this );
    }

    @Override
    public boolean isTrue()
    {
        return false;
    }

    @Override
    public boolean isFalse()
    {
        return false;
    }

    @Override
    public boolean isNull()
    {
        return false;
    }

    @Override
    public boolean containsKey( String key )
    {
        throw new NotMultiValued( type().name() + " is not a keyed collection" );
    }

    @Override
    public String asString()
    {
        throw new Uncoercible( type().name(), "Java String" );
    }

    @Override
    public long asLong()
    {
        throw new Uncoercible( type().name(), "Java long" );
    }

    @Override
    public int asInt()
    {
        throw new Uncoercible( type().name(), "Java int" );
    }

    @Override
    public float asFloat()
    {
        throw new Uncoercible( type().name(), "Java float" );
    }

    @Override
    public double asDouble()
    {
        throw new Uncoercible( type().name(), "Java double" );
    }

    @Override
    public boolean asBoolean()
    {
        throw new Uncoercible( type().name(), "Java boolean" );
    }

    @Override
    public List<Object> asList()
    {
        return asList( ofObject() );
    }

    @Override
    public <T> List<T> asList( Function<Value,T> mapFunction )
    {
        throw new Uncoercible( type().name(), "Java List" );
    }

    @Override
    public Map<String,Object> asMap()
    {
        return asMap( ofObject() );
    }

    @Override
    public <T> Map<String, T> asMap( Function<Value,T> mapFunction )
    {
        throw new Uncoercible( type().name(), "Java Map" );
    }

    @Override
    public Object asObject()
    {
        throw new Uncoercible( type().name(), "Java Object" );
    }

    @Override
    public byte[] asByteArray()
    {
        throw new Uncoercible( type().name(), "Byte array" );
    }

    @Override
    public Number asNumber()
    {
        throw new Uncoercible( type().name(), "Java Number" );
    }

    @Override
    public Entity asEntity()
    {
        throw new Uncoercible( type().name(), "Entity" );
    }

    @Override
    public Node asNode()
    {
        throw new Uncoercible( type().name(), "Node" );
    }

    @Override
    public Path asPath()
    {
        throw new Uncoercible( type().name(), "Path" );
    }

    @Override
    public Relationship asRelationship()
    {
        throw new Uncoercible( type().name(), "Relationship" );
    }

    @Override
    public LocalDate asLocalDate()
    {
        throw new Uncoercible( type().name(), "LocalDate" );
    }

    @Override
    public OffsetTime asOffsetTime()
    {
        throw new Uncoercible( type().name(), "OffsetTime" );
    }

    @Override
    public LocalTime asLocalTime()
    {
        throw new Uncoercible( type().name(), "LocalTime" );
    }

    @Override
    public LocalDateTime asLocalDateTime()
    {
        throw new Uncoercible( type().name(), "LocalDateTime" );
    }

    @Override
    public ZonedDateTime asZonedDateTime()
    {
        throw new Uncoercible( type().name(), "ZonedDateTime" );
    }

    @Override
    public CypherDuration asCypherDuration()
    {
        throw new Uncoercible( type().name(), "Duration" );
    }

    @Override
    public Point asPoint()
    {
        throw new Uncoercible( type().name(), "Point" );
    }

    @Override
    public Value get( int index )
    {
        throw new NotMultiValued( type().name() + " is not an indexed collection" );
    }

    @Override
    public Value get( String key )
    {
        throw new NotMultiValued( type().name() + " is not a keyed collection" );
    }

    @Override
    public int size()
    {
        throw new Unsizable( type().name() + " does not have size" );
    }

    @Override
    public Iterable<String> keys()
    {
        return emptyList();
    }

    @Override
    public boolean isEmpty()
    {
        return ! values().iterator().hasNext();
    }

    @Override
    public Iterable<Value> values()
    {
        return values( ofValue() );
    }

    @Override
    public <T> Iterable<T> values( Function<Value,T> mapFunction )
    {
        throw new NotMultiValued( type().name() + " is not iterable" );
    }

    @Override
    public final TypeConstructor typeConstructor()
    {
        return ( (TypeRepresentation) type() ).constructor();
    }

    // Force implementation
    @Override
    public abstract boolean equals( Object obj );

    // Force implementation
    @Override
    public abstract int hashCode();

    // Force implementation
    @Override
    public abstract String toString();
}


