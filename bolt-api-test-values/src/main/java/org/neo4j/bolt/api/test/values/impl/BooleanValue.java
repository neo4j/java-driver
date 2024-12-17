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

import org.neo4j.driver.internal.bolt.api.values.Type;

public abstract class BooleanValue extends ValueAdapter {
    private BooleanValue() {
        // do nothing
    }

    public static final BooleanValue TRUE = new TrueValue();
    public static final BooleanValue FALSE = new FalseValue();

    public static BooleanValue fromBoolean(boolean value) {
        return value ? TRUE : FALSE;
    }

    @Override
    public Type type() {
        return Type.BOOLEAN;
    }

    @Override
    public int hashCode() {
        var value = asBoolean() ? Boolean.TRUE : Boolean.FALSE;
        return value.hashCode();
    }

    private static class TrueValue extends BooleanValue {
        @Override
        public boolean asBoolean() {
            return true;
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public boolean equals(Object obj) {
            return obj == TRUE;
        }

        @Override
        public String toString() {
            return "TRUE";
        }
    }

    private static class FalseValue extends BooleanValue {
        @Override
        public boolean asBoolean() {
            return false;
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public boolean equals(Object obj) {
            return obj == FALSE;
        }

        @Override
        public String toString() {
            return "FALSE";
        }
    }
}
