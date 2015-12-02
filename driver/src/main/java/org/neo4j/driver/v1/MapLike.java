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

/**
 * Access the fields of an underlying unordered map like data structure by key
 */
public interface MapLike extends CollectionLike
{

    /**
     * Retrieve the keys of the underlying map
     *
     * @return all map keys in unspecified order
     */
    Iterable<String> keys();

    /**
     * Check if this map like contains a given key
     *
     * @param key the key
     * @return <tt>true</tt> if this map like contains the key otherwise <tt>false</tt>
     */
    boolean containsKey( String key );

    /**
     * Retrieve the value of the field with the given key
     *
     * @param key the key of the field
     * @return the field's value or a null value if no such key exists
     */
    Value value( String key );

    /**
     * Retrieve the entries of the underlying map
     *
     * @see org.neo4j.driver.v1.MapLike.Entry
     * @return all map entries in unspecified order
     */
    Iterable<Entry<Value>> entries();

    /**
     * Retrieve the entries of the underlying map
     *
     * @see org.neo4j.driver.v1.MapLike.Entry
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#valueAsBoolean()}, {@link Values#valueAsList(Function)}.
     * @param <V> the target type of mapping
     * @return all mapped map entries in unspecified order
     */
    <V> Iterable<Entry<V>> entries( Function<Value, V> mapFunction );

    /**
     * Immutable pair of a key and a value
     *
     * @param <V> the Java type of the contained value
     */
    interface Entry<V>
    {
        /**
         * @return the key of the entry
         */
        String key();

        /**
         * @return the value of the entry
         */
        V value();
    }
}
