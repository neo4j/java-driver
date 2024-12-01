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

import java.util.Arrays;
import java.util.Iterator;
import org.neo4j.driver.internal.bolt.api.values.Type;
import org.neo4j.driver.internal.bolt.api.values.Value;

public class ListValue extends ValueAdapter {
    private final Value[] values;

    public ListValue(Value... values) {
        if (values == null) {
            throw new IllegalArgumentException("Cannot construct ListValue from null");
        }
        this.values = values;
    }

    @Override
    public boolean isEmpty() {
        return values.length == 0;
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public Iterable<Value> values() {
        return () -> new Iterator<>() {
            private int cursor = 0;

            @Override
            public boolean hasNext() {
                return cursor < values.length;
            }

            @Override
            public Value next() {
                return values[cursor++];
            }

            @Override
            public void remove() {}
        };
    }

    @Override
    public Type type() {
        return Type.LIST;
    }

    @Override
    public String toString() {
        return Arrays.toString(values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        var otherValues = (ListValue) o;
        return Arrays.equals(values, otherValues.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }
}
