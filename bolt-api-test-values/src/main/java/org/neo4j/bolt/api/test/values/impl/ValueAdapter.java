/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.bolt.api.test.values.impl;

import static java.util.Collections.emptyList;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.function.Function;
import org.neo4j.bolt.api.test.values.TestNode;
import org.neo4j.bolt.api.test.values.TestPath;
import org.neo4j.bolt.api.test.values.TestRelationship;
import org.neo4j.bolt.api.test.values.TestValue;
import org.neo4j.driver.internal.bolt.api.values.IsoDuration;
import org.neo4j.driver.internal.bolt.api.values.Point;
import org.neo4j.driver.internal.bolt.api.values.Value;

public abstract class ValueAdapter extends InternalMapAccessorWithDefaultValue implements TestValue {
    @Override
    public Value asValue() {
        return this;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public boolean containsKey(String key) {
        throw new NotMultiValued(type().name() + " is not a keyed collection");
    }

    @Override
    public String asString() {
        throw new Uncoercible(type().name(), "Java String");
    }

    @Override
    public long asLong() {
        throw new Uncoercible(type().name(), "Java long");
    }

    @Override
    public double asDouble() {
        throw new Uncoercible(type().name(), "Java double");
    }

    @Override
    public boolean asBoolean() {
        throw new Uncoercible(type().name(), "Java boolean");
    }

    @Override
    public <T> Map<String, T> asMap(Function<Value, T> mapFunction) {
        throw new Uncoercible(type().name(), "Java Map");
    }

    @Override
    public LocalDate asLocalDate() {
        throw new Uncoercible(type().name(), "LocalDate");
    }

    @Override
    public OffsetTime asOffsetTime() {
        throw new Uncoercible(type().name(), "OffsetTime");
    }

    @Override
    public LocalTime asLocalTime() {
        throw new Uncoercible(type().name(), "LocalTime");
    }

    @Override
    public LocalDateTime asLocalDateTime() {
        throw new Uncoercible(type().name(), "LocalDateTime");
    }

    @Override
    public ZonedDateTime asZonedDateTime() {
        throw new Uncoercible(type().name(), "ZonedDateTime");
    }

    @Override
    public IsoDuration asIsoDuration() {
        throw new Uncoercible(type().name(), "Duration");
    }

    @Override
    public Point asPoint() {
        throw new Uncoercible(type().name(), "Point");
    }

    @Override
    public Value get(String key) {
        throw new NotMultiValued(type().name() + " is not a keyed collection");
    }

    @Override
    public int size() {
        throw new Unsizable(type().name() + " does not have size");
    }

    @Override
    public Iterable<String> keys() {
        return emptyList();
    }

    @Override
    public boolean isEmpty() {
        return !values().iterator().hasNext();
    }

    @Override
    public Iterable<Value> values() {
        throw new NotMultiValued(type().name() + " is not iterable");
    }

    @Override
    public byte[] asByteArray() {
        throw new Uncoercible(type().name(), "byte[]");
    }

    @Override
    public TestNode asNode() {
        throw new Uncoercible(type().name(), "Node");
    }

    @Override
    public TestRelationship asRelationship() {
        throw new Uncoercible(type().name(), "Relationship");
    }

    @Override
    public TestPath asPath() {
        throw new Uncoercible(type().name(), "Path");
    }

    // Force implementation
    @Override
    public abstract boolean equals(Object obj);

    // Force implementation
    @Override
    public abstract int hashCode();

    // Force implementation
    @Override
    public abstract String toString();
}
