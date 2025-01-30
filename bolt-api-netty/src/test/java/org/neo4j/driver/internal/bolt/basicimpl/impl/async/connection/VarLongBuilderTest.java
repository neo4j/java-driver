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
package org.neo4j.driver.internal.bolt.basicimpl.impl.async.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class VarLongBuilderTest {
    VarLongBuilder builder;

    @BeforeEach
    void beforeEach() {
        builder = new VarLongBuilder();
    }

    @ParameterizedTest
    @MethodSource("shouldBuildArgs")
    void shouldBuild(byte[] segments, long value) {
        for (var segment : segments) {
            builder.add(segment);
        }

        assertEquals(value, builder.build());
    }

    @Test
    void shouldThrowOnOverflow() {
        var segments = new byte[] {127, 127, 127, 127, 127, 127, 127, 127, 127};
        for (var segment : segments) {
            builder.add(segment);
        }

        assertThrows(IllegalStateException.class, () -> builder.add(0));
    }

    private static Stream<Arguments> shouldBuildArgs() {
        return Stream.of(
                arguments(new byte[] {0}, 0L),
                arguments(new byte[] {127}, 127L),
                arguments(new byte[] {(byte) 0b00010110, 1}, 150L),
                arguments(new byte[] {127, 127, 127, 127, (byte) 0b00000111}, Integer.MAX_VALUE),
                arguments(new byte[] {127, 127, 127, 127, 127, 127, 127, 127, 127}, Long.MAX_VALUE));
    }
}
