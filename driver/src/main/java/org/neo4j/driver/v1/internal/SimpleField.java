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

import org.neo4j.driver.v1.Field;

public class SimpleField<V> extends SimpleProperty<V> implements Field<V>
{
    private final int index;

    public SimpleField( String key, int index, V value )
    {
        super( key, value );
        this.index = index;
    }

    @Override
    public int index()
    {
        return index;
    }

    public static <V> Field<V> of( String key, int index, V value )
    {
        return new SimpleField<>( key, index, value );
    }
}
