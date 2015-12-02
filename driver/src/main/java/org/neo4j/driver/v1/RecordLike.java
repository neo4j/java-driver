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
package org.neo4j.driver.v1;

import java.util.List;

/**
 * Access the entries of an underlying ordered map like data structure by key and index
 */
public interface RecordLike extends ListLike, MapLike
{
    @Override
    List<String> keys();

    /**
     * Retrieve all entry values of the record
     *
     * @return all entry values in key order
     */
    @Override
    List<Value> values();

    /**
     * Map and retrieve all entry values of the record
     *
     * @param <T> the target type of the map function
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#valueAsBoolean()}, {@link Values#valueAsList(Function)}.
     * @return the result of mapping all entry values of the record in key order
     */
    @Override
    <T> List<T> values( Function<Value, T> mapFunction );

    /**
     * Retrieve all entries of the record
     *
     * @return all entries in key order
     */
    @Override
    List<Entry<Value>> entries();

    /**
     * Map and retrieve all entries of the record
     *
     * @param <V> the target type of the map function
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#valueAsBoolean()}, {@link Values#valueAsList(Function)}.
     * @return the result of mapping all entries of the record in key order
     */
    @Override
    <V> List<Entry<V>> entries( Function<Value, V> mapFunction );
}
