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
package org.neo4j.driver.internal.bolt.basicimpl.impl.util;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.BoltProtocol;
import org.neo4j.driver.internal.bolt.basicimpl.impl.spi.Connection;

public class TestUtil {
    private static final long DEFAULT_WAIT_TIME_MS = MINUTES.toMillis(100);

    public static Connection connectionMock(BoltProtocol protocol) {
        var connection = mock(Connection.class);
        given(connection.protocol()).willReturn(protocol);
        return connection;
    }

    public static void assertByteBufEquals(ByteBuf expected, ByteBuf actual) {
        try {
            assertEquals(expected, actual);
        } finally {
            releaseIfPossible(expected);
            releaseIfPossible(actual);
        }
    }

    public static <T> T await(CompletableFuture<T> future) {
        return await((Future<T>) future);
    }

    public static <T, U extends Future<T>> T await(U future) {
        try {
            return future.get(DEFAULT_WAIT_TIME_MS, MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for future: " + future, e);
        } catch (ExecutionException e) {
            PlatformDependent.throwException(e.getCause());
            return null;
        } catch (TimeoutException e) {
            throw new AssertionError("Given future did not complete in time: " + future);
        }
    }

    public static void assertByteBufContains(ByteBuf buf, Number... values) {
        try {
            assertNotNull(buf);
            var expectedReadableBytes =
                    Arrays.stream(values).mapToInt(TestUtil::bytesCount).sum();
            assertEquals(expectedReadableBytes, buf.readableBytes(), "Unexpected number of bytes");
            for (var expectedValue : values) {
                var actualValue = read(buf, expectedValue.getClass());
                var valueType = actualValue.getClass().getSimpleName();
                assertEquals(expectedValue, actualValue, valueType + " values not equal");
            }
        } finally {
            releaseIfPossible(buf);
        }
    }

    private static Number read(ByteBuf buf, Class<? extends Number> type) {
        if (type == Byte.class) {
            return buf.readByte();
        } else if (type == Short.class) {
            return buf.readShort();
        } else if (type == Integer.class) {
            return buf.readInt();
        } else if (type == Long.class) {
            return buf.readLong();
        } else if (type == Float.class) {
            return buf.readFloat();
        } else if (type == Double.class) {
            return buf.readDouble();
        } else {
            throw new IllegalArgumentException("Unexpected numeric type: " + type);
        }
    }

    private static int bytesCount(Number value) {
        if (value instanceof Byte) {
            return 1;
        } else if (value instanceof Short) {
            return 2;
        } else if (value instanceof Integer) {
            return 4;
        } else if (value instanceof Long) {
            return 8;
        } else if (value instanceof Float) {
            return 4;
        } else if (value instanceof Double) {
            return 8;
        } else {
            throw new IllegalArgumentException("Unexpected number: '" + value + "' or type" + value.getClass());
        }
    }

    private static void releaseIfPossible(ByteBuf buf) {
        if (buf.refCnt() > 0) {
            buf.release();
        }
    }
}
