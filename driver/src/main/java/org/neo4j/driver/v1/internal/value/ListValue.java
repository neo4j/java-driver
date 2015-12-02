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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.neo4j.driver.v1.Function;
import org.neo4j.driver.v1.Type;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.internal.types.StandardTypeSystem;
import org.neo4j.driver.v1.internal.types.TypeConstructor;
import org.neo4j.driver.v1.internal.util.Extract;

public class ListValue extends ValueAdapter
{
    private final Value[] values;

    public ListValue( Value... values )
    {
        this.values = values;
    }

    @Override
    public List<Value> asList()
    {
        return Extract.list( values );
    }

    @Override
    public <T> List<T> asList( Function<Value,T> mapFunction )
    {
        return Extract.list( this, mapFunction );
    }

    public Object asObject()
    {
        return asList();
    }

    @Override
    public boolean isList()
    {
        return true;
    }

    @Override
    public int fieldCount()
    {
        return values.length;
    }

    @Override
    public TypeConstructor typeConstructor()
    {
        return TypeConstructor.LIST_TyCon;
    }

    @Override
    public Value value( int index )
    {
        return values[index];
    }

    @Override
    public <T> Iterable<T> values( final Function<Value,T> mapFunction )
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return new Iterator<T>()
                {
                    private int cursor = 0;

                    @Override
                    public boolean hasNext()
                    {
                        return cursor < values.length;
                    }

                    @Override
                    public T next()
                    {
                        return mapFunction.apply( values[cursor++] );
                    }

                    @Override
                    public void remove()
                    {
                    }
                };
            }
        };
    }

    @Override
    public Type type()
    {
        return StandardTypeSystem.TYPE_SYSTEM.LIST();
    }

    @Override
    public String toString()
    {
        return "ListValue" + Arrays.toString( values ) + "";
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

        ListValue values1 = (ListValue) o;

        return Arrays.equals( values, values1.values );

    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( values );
    }
}
