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

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.neo4j.driver.v1.Function;
import org.neo4j.driver.v1.Type;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.internal.types.StandardTypeSystem;
import org.neo4j.driver.v1.internal.util.Extract;

import static org.neo4j.driver.v1.Values.valueAsString;

public class ListValue extends ValueAdapter
{
    private final Value[] values;

    public ListValue( Value... values )
    {
        this.values = values;
    }

    @Override
    public boolean isEmpty()
    {
        return values.length == 0;
    }

    @Override
    public List<Value> asList()
    {
        return Extract.list( values );
    }

    @Override
    public <T> List<T> asList( Function<Value,T> mapFunction )
    {
        return Extract.list( values, mapFunction );
    }

    public Object asObject()
    {
        return asList();
    }

    @Override
    public Value[] asArray()
    {
        int size = size();
        Value[] result = new Value[size];
        System.arraycopy( values, 0, result, 0, size );
        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T[] asArray( Class<T> clazz, Function<Value, T> mapFunction )
    {
        int size = size();
        T[] result = (T[]) Array.newInstance( clazz, size );
        for ( int i = 0; i < size; i++ )
        {
            result[i] = mapFunction.apply( values[i] );
        }
        return result;
    }

    @Override
    public long[] asLongArray()
    {
        long[] result = new long[ size() ];
        for ( int i = 0; i < values.length; i++ )
        {
            result[i] = values[i].asLong();
        }
        return result;
    }

    @Override
    public int[] asIntArray()
    {
        int[] result = new int[ size() ];
        for ( int i = 0; i < values.length; i++ )
        {
            result[i] = values[i].asInt();
        }
        return result;
    }

    @Override
    public short[] asShortArray()
    {
        short[] result = new short[ size() ];
        for ( int i = 0; i < values.length; i++ )
        {
            result[i] = values[i].asShort();
        }
        return result;
    }

    @Override
    public byte[] asByteArray()
    {
        byte[] result = new byte[ size() ];
        for ( int i = 0; i < values.length; i++ )
        {
            result[i] = values[i].asByte();
        }
        return result;
    }

    @Override
    public double[] asDoubleArray()
    {
        double[] result = new double[ size() ];
        for ( int i = 0; i < values.length; i++ )
        {
            result[i] = values[i].asDouble();
        }
        return result;
    }

    @Override
    public float[] asFloatArray()
    {
        float[] result = new float[ size() ];
        for ( int i = 0; i < values.length; i++ )
        {
            result[i] = values[i].asFloat();
        }
        return result;
    }

    @Override
    public int size()
    {
        return values.length;
    }

    @Override
    public Value value( int index )
    {
        return index >= 0 && index < values.length ? values[index] : Values.NULL;
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
    public String asString()
    {
        return asList( valueAsString() ).toString();
    }

    @Override
    public String asLiteralString()
    {
        return Arrays.toString( values );
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

        ListValue otherValues = (ListValue) o;
        return Arrays.equals( values, otherValues.values );
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( values );
    }
}
