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
package org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v57;

import static java.util.Arrays.asList;
import static java.util.Calendar.APRIL;
import static java.util.Calendar.AUGUST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.bolt.api.values.Value;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.Message;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.MessageFormat;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.request.DiscardAllMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.response.IgnoredMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.response.RecordMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.response.SuccessMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.packstream.PackInput;
import org.neo4j.driver.internal.bolt.basicimpl.impl.util.messaging.AbstractMessageReaderTestBase;

class MessageReaderV57Test extends AbstractMessageReaderTestBase {
    private final Map<String, Value> DIAGNOSTIC_RECORD = Map.ofEntries(
            Map.entry("OPERATION", valueFactory.value("")),
            Map.entry("OPERATION_CODE", valueFactory.value("0")),
            Map.entry("CURRENT_SCHEMA", valueFactory.value("/")));

    @Override
    protected Stream<Message> supportedMessages() {
        return Stream.of(
                // V2 Record types
                record(valueFactory.point(42, 120.65, -99.2)),
                record(valueFactory.point(42, 85.391, 98.8, 11.1)),
                record(valueFactory.value(LocalDate.of(2012, AUGUST, 3))),
                record(valueFactory.value(OffsetTime.of(23, 59, 59, 999, ZoneOffset.MAX))),
                record(valueFactory.value(LocalTime.of(12, 25))),
                record(valueFactory.value(LocalDateTime.of(1999, APRIL, 3, 19, 5, 5, 100_200_300))),
                record(valueFactory.isoDuration(
                        Long.MAX_VALUE - 1, Integer.MAX_VALUE - 1, Short.MAX_VALUE - 1, Byte.MAX_VALUE - 1)),
                record(valueFactory.isoDuration(17, 22, 99, 15)),

                // Bolt previous versions valid messages
                IgnoredMessage.IGNORED,
                new SuccessMessage(new HashMap<>()),
                record(valueFactory.value(1337L)),
                record(valueFactory.value(List.of("cat", valueFactory.value(null), "dog", valueFactory.value(null)))),
                record(valueFactory.value(List.of("k", valueFactory.value(12), "a", valueFactory.value("banana")))),
                record(valueFactory.value(asList(
                        valueFactory.value("k"),
                        valueFactory.value(12),
                        valueFactory.value("a"),
                        valueFactory.value("banana")))));
    }

    @Override
    protected Stream<Message> unsupportedMessages() {
        return Stream.of(DiscardAllMessage.DISCARD_ALL);
    }

    @Override
    protected MessageFormat.Reader newReader(PackInput input) {
        return BoltProtocolV57.INSTANCE.createMessageFormat().newReader(input, valueFactory);
    }

    @Test
    void shouldInitGqlError() {
        var messageReader = new MessageReaderV57(mock(), valueFactory);
        var gqlStatus = valueFactory.value("gql_status");
        var description = valueFactory.value("description");
        var message = valueFactory.value("message");
        var params = Map.of(
                "gql_status", gqlStatus,
                "description", description,
                "message", message);

        var gqlError = messageReader.unpackGqlError(params);

        assertEquals(gqlStatus.asString(), gqlError.gqlStatus());
        assertEquals(description.asString(), gqlError.statusDescription());
        assertEquals("N/A", gqlError.code());
        assertEquals(message.asString(), gqlError.message());
        assertEquals(DIAGNOSTIC_RECORD, gqlError.diagnosticRecord());
        assertNull(gqlError.cause());
    }

    @Test
    void shouldInitGqlErrorWithMap() {
        var messageReader = new MessageReaderV57(mock(), valueFactory);
        var gqlStatus = valueFactory.value("gql_status");
        var description = valueFactory.value("description");
        var message = valueFactory.value("message");
        var map = Map.of("key", valueFactory.value("value"));
        var params = Map.of(
                "gql_status", gqlStatus,
                "description", description,
                "message", message,
                "diagnostic_record", valueFactory.value(map));

        var gqlError = messageReader.unpackGqlError(params);

        assertEquals(gqlStatus.asString(), gqlError.gqlStatus());
        assertEquals(description.asString(), gqlError.statusDescription());
        assertEquals("N/A", gqlError.code());
        assertEquals(message.asString(), gqlError.message());
        var test = new HashMap<>(DIAGNOSTIC_RECORD);
        test.putAll(map);
        assertEquals(test, gqlError.diagnosticRecord());
        assertNull(gqlError.cause());
    }

    private Message record(Value value) {
        return new RecordMessage(new Value[] {value});
    }
}
