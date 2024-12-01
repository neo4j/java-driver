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
package org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v41;

import static java.util.Arrays.asList;
import static java.util.Calendar.APRIL;
import static java.util.Calendar.AUGUST;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;
import org.neo4j.driver.internal.bolt.api.values.Value;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.Message;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.MessageFormat;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.request.DiscardAllMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.response.FailureMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.response.IgnoredMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.response.RecordMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.response.SuccessMessage;
import org.neo4j.driver.internal.bolt.basicimpl.impl.packstream.PackInput;
import org.neo4j.driver.internal.bolt.basicimpl.impl.util.messaging.AbstractMessageReaderTestBase;

/**
 * The MessageReader under tests is the one provided by the {@link BoltProtocolV41} and not an specific class implementation.
 * <p>
 * It's done on this way to make easy to replace the implementation and still getting the same behaviour.
 */
public class MessageReaderV41Test extends AbstractMessageReaderTestBase {

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
                record(valueFactory.value(
                        ZonedDateTime.of(1823, 1, 12, 23, 59, 59, 999_999_999, ZoneOffset.ofHoursMinutes(-7, -15)))),
                record(valueFactory.value(
                        ZonedDateTime.of(1823, 1, 12, 23, 59, 59, 999_999_999, ZoneId.of("Europe/Stockholm")))),
                record(valueFactory.isoDuration(
                        Long.MAX_VALUE - 1, Integer.MAX_VALUE - 1, Short.MAX_VALUE - 1, Byte.MAX_VALUE - 1)),
                record(valueFactory.isoDuration(17, 22, 99, 15)),

                // Bolt previous versions valid messages
                new FailureMessage("Hello", "World!"),
                IgnoredMessage.IGNORED,
                new SuccessMessage(new HashMap<>()),
                record(valueFactory.value(1337L)),
                record(valueFactory.value(List.of("cat", valueFactory.value(null), "dog", valueFactory.value(null)))),
                record(valueFactory.value(List.of("k", valueFactory.value(12), "a", valueFactory.value("banana")))),
                record(valueFactory.value(asList(
                        valueFactory.value("k"),
                        valueFactory.value(12),
                        valueFactory.value("a"),
                        valueFactory.value("banana")))),

                // V3 Record Types
                record(valueFactory.emptyNodeValue()),
                record(valueFactory.filledNodeValue()),
                record(valueFactory.emptyRelationshipValue()),
                record(valueFactory.filledRelationshipValue()),
                record(valueFactory.filledPathValue()),
                record(valueFactory.emptyPathValue()));
    }

    @Override
    protected Stream<Message> unsupportedMessages() {
        return Stream.of(DiscardAllMessage.DISCARD_ALL);
    }

    @Override
    protected MessageFormat.Reader newReader(PackInput input) {
        return BoltProtocolV41.INSTANCE.createMessageFormat().newReader(input, valueFactory);
    }

    private Message record(Value value) {
        return new RecordMessage(new Value[] {value});
    }
}
