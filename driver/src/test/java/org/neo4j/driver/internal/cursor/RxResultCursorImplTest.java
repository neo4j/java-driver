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
package org.neo4j.driver.internal.cursor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.MockitoAnnotations.openMocks;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;

class RxResultCursorImplTest {
    @Mock
    BoltConnection connection;

    @Mock
    Query query;

    @Mock
    RunSummary runSummary;

    @Mock
    Consumer<DatabaseBookmark> bookmarkConsumer;

    @Mock
    Consumer<Throwable> throwableConsumer;

    @Mock
    Supplier<Throwable> termSupplier;

    @BeforeEach
    @SuppressWarnings("resource")
    void beforeEach() {
        openMocks(this);
        given(connection.protocolVersion()).willReturn(new BoltProtocolVersion(5, 5));
    }

    @Test
    void shouldNotifyRecordConsumerOfRunError() {
        // given
        var runError = mock(Throwable.class);
        var cursor = new RxResultCursorImpl(
                connection,
                query,
                null,
                runError,
                bookmarkConsumer,
                throwableConsumer,
                false,
                termSupplier,
                Logging.none());
        @SuppressWarnings("unchecked")
        BiConsumer<Record, Throwable> recordConsumer = mock(BiConsumer.class);
        cursor.installRecordConsumer(recordConsumer);

        // when
        cursor.request(1);

        // then
        then(recordConsumer).should().accept(null, runError);
    }

    @Test
    void shouldNotNotifyRecordConsumerOfRunErrorWhenRunErrorIsRequested() {
        // given
        var runError = mock(Throwable.class);
        var cursor = new RxResultCursorImpl(
                connection,
                query,
                runSummary,
                runError,
                bookmarkConsumer,
                throwableConsumer,
                false,
                termSupplier,
                Logging.none());
        @SuppressWarnings("unchecked")
        BiConsumer<Record, Throwable> recordConsumer = mock(BiConsumer.class);
        assertEquals(runError, cursor.getRunError());

        // when
        cursor.installRecordConsumer(recordConsumer);

        // then
        then(recordConsumer).shouldHaveNoInteractions();
    }

    @Test
    void shouldReturnKeys() {
        // given
        var keys = List.of("a", "b");
        given(runSummary.keys()).willReturn(keys);
        var cursor = new RxResultCursorImpl(
                connection,
                query,
                runSummary,
                null,
                bookmarkConsumer,
                throwableConsumer,
                false,
                termSupplier,
                Logging.none());

        // when & then
        assertEquals(keys, cursor.keys());
        then(runSummary).should().keys();
    }
}
