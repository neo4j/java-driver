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
package org.neo4j.driver.internal.value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.neo4j.driver.Function;
import org.neo4j.driver.Value;

public class ListValue extends ValueAdapter
{
    private final Value[] values;

    public ListValue( Value... values )
    {
        this.values = values;
    }

    @Override
    public boolean javaBoolean()
    {
        return values.length > 0;
    }

    @Override
    public <T> List<T> javaList( Function<Value,T> map )
    {
        List<T> list = new ArrayList<>( values.length );
        for ( Value value : values )
        {
            list.add( map.apply( value ) );
        }
        return list;
    }

    @Override
    public boolean isList()
    {
        return true;
    }

    @Override
    public long size()
    {
        return values.length;
    }

    @Override
    public Value get( long index )
    {
        return values[(int) index];
    }

    @Override
    public Iterator<Value> iterator()
    {
        return new Iterator<Value>()
        {
            private int cursor = 0;

            @Override
            public boolean hasNext()
            {
                return cursor < values.length;
            }

            @Override
            public Value next()
            {
                return values[cursor++];
            }

            @Override
            public void remove()
            {
            }
        };
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

    @Override
    public String toString()
    {
        return "ListValue" + Arrays.toString( values ) + "";
    }
}
