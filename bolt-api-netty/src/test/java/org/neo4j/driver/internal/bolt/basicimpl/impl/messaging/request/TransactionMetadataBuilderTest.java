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
package org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.request;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.bolt.api.AccessMode.READ;
import static org.neo4j.driver.internal.bolt.api.AccessMode.WRITE;
import static org.neo4j.driver.internal.bolt.api.DatabaseNameUtil.database;
import static org.neo4j.driver.internal.bolt.api.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.request.TransactionMetadataBuilder.buildMetadata;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.bolt.api.test.values.TestValueFactory;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.NotificationClassification;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.NotificationSeverity;
import org.neo4j.driver.internal.bolt.api.values.Value;
import org.neo4j.driver.internal.bolt.api.values.ValueFactory;
import org.neo4j.driver.internal.bolt.basicimpl.impl.NoopLoggingProvider;

public class TransactionMetadataBuilderTest {
    private static final ValueFactory valueFactory = TestValueFactory.INSTANCE;

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldHaveCorrectMetadata(AccessMode mode) {
        var bookmarks = new HashSet<>(asList("neo4j:bookmark:v1:tx11", "neo4j:bookmark:v1:tx52"));

        Map<String, Value> txMetadata = new HashMap<>();
        txMetadata.put("foo", valueFactory.value("bar"));
        txMetadata.put("baz", valueFactory.value(111));
        txMetadata.put("time", valueFactory.value(LocalDateTime.now()));

        var txTimeout = Duration.ofSeconds(7);

        var metadata = buildMetadata(
                txTimeout,
                txMetadata,
                defaultDatabase(),
                mode,
                bookmarks,
                null,
                null,
                null,
                false,
                NoopLoggingProvider.INSTANCE,
                valueFactory);

        Map<String, Value> expectedMetadata = new HashMap<>();
        expectedMetadata.put(
                "bookmarks",
                valueFactory.value(bookmarks.stream().map(valueFactory::value).collect(Collectors.toSet())));
        expectedMetadata.put("tx_timeout", valueFactory.value(7000));
        expectedMetadata.put("tx_metadata", valueFactory.value(txMetadata));
        if (mode == READ) {
            expectedMetadata.put("mode", valueFactory.value("r"));
        }

        assertEquals(expectedMetadata, metadata);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "foo", "data"})
    void shouldHaveCorrectMetadataForDatabaseName(String databaseName) {
        var bookmarks = new HashSet<>(asList("neo4j:bookmark:v1:tx11", "neo4j:bookmark:v1:tx52"));

        Map<String, Value> txMetadata = new HashMap<>();
        txMetadata.put("foo", valueFactory.value("bar"));
        txMetadata.put("baz", valueFactory.value(111));
        txMetadata.put("time", valueFactory.value(LocalDateTime.now()));

        var txTimeout = Duration.ofSeconds(7);

        var metadata = buildMetadata(
                txTimeout,
                txMetadata,
                database(databaseName),
                WRITE,
                bookmarks,
                null,
                null,
                null,
                false,
                NoopLoggingProvider.INSTANCE,
                valueFactory);

        Map<String, Value> expectedMetadata = new HashMap<>();
        expectedMetadata.put(
                "bookmarks",
                valueFactory.value(bookmarks.stream().map(valueFactory::value).collect(Collectors.toSet())));
        expectedMetadata.put("tx_timeout", valueFactory.value(7000));
        expectedMetadata.put("tx_metadata", valueFactory.value(txMetadata));
        expectedMetadata.put("db", valueFactory.value(databaseName));

        assertEquals(expectedMetadata, metadata);
    }

    @Test
    void shouldNotHaveMetadataForDatabaseNameWhenIsNull() {
        var metadata = buildMetadata(
                null,
                null,
                defaultDatabase(),
                WRITE,
                Collections.emptySet(),
                null,
                null,
                null,
                false,
                NoopLoggingProvider.INSTANCE,
                valueFactory);
        assertTrue(metadata.isEmpty());
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    void shouldIncludeNotificationConfig() {
        var metadata = buildMetadata(
                null,
                null,
                defaultDatabase(),
                WRITE,
                Collections.emptySet(),
                null,
                null,
                new NotificationConfig(
                        NotificationSeverity.WARNING,
                        Set.of(NotificationClassification.valueOf("UNSUPPORTED").get())),
                false,
                NoopLoggingProvider.INSTANCE,
                valueFactory);

        var expectedMetadata = new HashMap<String, Value>();
        expectedMetadata.put("notifications_minimum_severity", valueFactory.value("WARNING"));
        expectedMetadata.put("notifications_disabled_classifications", valueFactory.value(Set.of("UNSUPPORTED")));
        assertEquals(expectedMetadata, metadata);
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 1_000_001, 100_500_000, 100_700_000, 1_000_000_001})
    void shouldRoundUpFractionalTimeoutAndLog(long nanosValue) {
        // given
        var logging = mock(LoggingProvider.class);
        var logger = mock(System.Logger.class);
        given(logging.getLog(TransactionMetadataBuilder.class)).willReturn(logger);

        // when
        var metadata = buildMetadata(
                Duration.ofNanos(nanosValue),
                null,
                defaultDatabase(),
                WRITE,
                Collections.emptySet(),
                null,
                null,
                null,
                false,
                logging,
                valueFactory);

        // then
        var expectedMetadata = new HashMap<String, Value>();
        var expectedMillis = nanosValue / 1_000_000 + 1;
        expectedMetadata.put("tx_timeout", valueFactory.value(expectedMillis));
        assertEquals(expectedMetadata, metadata);
        then(logging).should().getLog(TransactionMetadataBuilder.class);
        then(logger)
                .should()
                .log(
                        System.Logger.Level.INFO,
                        "The transaction timeout has been rounded up to next millisecond value since the config had a fractional millisecond value");
    }

    @Test
    void shouldNotLogWhenRoundingDoesNotHappen() {
        // given
        var logging = mock(LoggingProvider.class);
        var logger = mock(System.Logger.class);
        given(logging.getLog(TransactionMetadataBuilder.class)).willReturn(logger);
        var timeout = 1000;

        // when
        var metadata = buildMetadata(
                Duration.ofMillis(timeout),
                null,
                defaultDatabase(),
                WRITE,
                Collections.emptySet(),
                null,
                null,
                null,
                false,
                logging,
                valueFactory);

        // then
        var expectedMetadata = new HashMap<String, Value>();
        expectedMetadata.put("tx_timeout", valueFactory.value(timeout));
        assertEquals(expectedMetadata, metadata);
        then(logging).shouldHaveNoInteractions();
        then(logger).shouldHaveNoInteractions();
    }
}
