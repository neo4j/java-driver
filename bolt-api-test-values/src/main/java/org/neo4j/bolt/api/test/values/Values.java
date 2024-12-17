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
package org.neo4j.bolt.api.test.values;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.neo4j.bolt.api.test.values.impl.AsValue;
import org.neo4j.bolt.api.test.values.impl.BooleanValue;
import org.neo4j.bolt.api.test.values.impl.BytesValue;
import org.neo4j.bolt.api.test.values.impl.DateTimeValue;
import org.neo4j.bolt.api.test.values.impl.DateValue;
import org.neo4j.bolt.api.test.values.impl.DurationValue;
import org.neo4j.bolt.api.test.values.impl.FloatValue;
import org.neo4j.bolt.api.test.values.impl.IntegerValue;
import org.neo4j.bolt.api.test.values.impl.InternalIsoDuration;
import org.neo4j.bolt.api.test.values.impl.InternalPoint2D;
import org.neo4j.bolt.api.test.values.impl.InternalPoint3D;
import org.neo4j.bolt.api.test.values.impl.ListValue;
import org.neo4j.bolt.api.test.values.impl.LocalDateTimeValue;
import org.neo4j.bolt.api.test.values.impl.LocalTimeValue;
import org.neo4j.bolt.api.test.values.impl.MapValue;
import org.neo4j.bolt.api.test.values.impl.NullValue;
import org.neo4j.bolt.api.test.values.impl.PointValue;
import org.neo4j.bolt.api.test.values.impl.StringValue;
import org.neo4j.bolt.api.test.values.impl.TimeValue;
import org.neo4j.driver.internal.bolt.api.values.IsoDuration;
import org.neo4j.driver.internal.bolt.api.values.Point;
import org.neo4j.driver.internal.bolt.api.values.Value;

final class Values {
    public static final Value EmptyMap = value(Collections.emptyMap());

    private Values() {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a value from object.
     *
     * @param value the object value
     * @return the array of values
     */
    @SuppressWarnings("unchecked")
    public static Value value(Object value) {
        if (value == null) {
            return NullValue.NULL;
        }

        if (value instanceof AsValue) {
            return ((AsValue) value).asValue();
        }
        if (value instanceof Boolean) {
            return value((boolean) value);
        }
        if (value instanceof String) {
            return value((String) value);
        }
        if (value instanceof Character) {
            return value((char) value);
        }
        if (value instanceof Long) {
            return value((long) value);
        }
        if (value instanceof Short) {
            return value((short) value);
        }
        if (value instanceof Byte) {
            return value((byte) value);
        }
        if (value instanceof Integer) {
            return value((int) value);
        }
        if (value instanceof Double) {
            return value((double) value);
        }
        if (value instanceof Float) {
            return value((float) value);
        }
        if (value instanceof LocalDate) {
            return value((LocalDate) value);
        }
        if (value instanceof OffsetTime) {
            return value((OffsetTime) value);
        }
        if (value instanceof LocalTime) {
            return value((LocalTime) value);
        }
        if (value instanceof LocalDateTime) {
            return value((LocalDateTime) value);
        }
        if (value instanceof OffsetDateTime) {
            return value((OffsetDateTime) value);
        }
        if (value instanceof ZonedDateTime) {
            return value((ZonedDateTime) value);
        }
        if (value instanceof IsoDuration) {
            return value((IsoDuration) value);
        }
        if (value instanceof Period) {
            return value((Period) value);
        }
        if (value instanceof Duration) {
            return value((Duration) value);
        }
        if (value instanceof Point) {
            return value((Point) value);
        }

        if (value instanceof List<?>) {
            return value((List<Object>) value);
        }
        if (value instanceof Map<?, ?>) {
            return value((Map<String, Object>) value);
        }
        if (value instanceof Iterable<?>) {
            return value((Iterable<Object>) value);
        }
        if (value instanceof Iterator<?>) {
            return value((Iterator<Object>) value);
        }
        if (value instanceof Stream<?>) {
            return value((Stream<Object>) value);
        }

        if (value instanceof char[]) {
            return value((char[]) value);
        }
        if (value instanceof byte[]) {
            return value((byte[]) value);
        }
        if (value instanceof boolean[]) {
            return value((boolean[]) value);
        }
        if (value instanceof String[]) {
            return value((String[]) value);
        }
        if (value instanceof long[]) {
            return value((long[]) value);
        }
        if (value instanceof int[]) {
            return value((int[]) value);
        }
        if (value instanceof short[]) {
            return value((short[]) value);
        }
        if (value instanceof double[]) {
            return value((double[]) value);
        }
        if (value instanceof float[]) {
            return value((float[]) value);
        }
        if (value instanceof Value[]) {
            return value((Value[]) value);
        }
        if (value instanceof Object[]) {
            return value(Arrays.asList((Object[]) value));
        }

        throw new IllegalArgumentException(
                "Unsupported value type: " + value.getClass().getName());
    }

    /**
     * Returns an array of values from object vararg.
     *
     * @param input the object value(s)
     * @return the array of values
     */
    public static Value[] values(final Object... input) {
        return Arrays.stream(input).map(Values::value).toArray(Value[]::new);
    }

    /**
     * Returns a value from value vararg.
     *
     * @param input the value(s)
     * @return the value
     */
    public static Value value(Value... input) {
        var size = input.length;
        var values = new Value[size];
        System.arraycopy(input, 0, values, 0, size);
        return new ListValue(values);
    }

    /**
     * Returns a value from byte vararg.
     *
     * @param input the byte value(s)
     * @return the value
     */
    public static Value value(byte... input) {
        return new BytesValue(input);
    }

    /**
     * Returns a value from string vararg.
     *
     * @param input the string value(s)
     * @return the value
     */
    public static Value value(String... input) {
        var values = Arrays.stream(input).map(StringValue::new).toArray(StringValue[]::new);
        return new ListValue(values);
    }

    /**
     * Returns a value from boolean vararg.
     *
     * @param input the boolean value(s)
     * @return the value
     */
    public static Value value(boolean... input) {
        var values =
                IntStream.range(0, input.length).mapToObj(i -> value(input[i])).toArray(Value[]::new);
        return new ListValue(values);
    }

    /**
     * Returns a value from char vararg.
     *
     * @param input the char value(s)
     * @return the value
     */
    public static Value value(char... input) {
        var values =
                IntStream.range(0, input.length).mapToObj(i -> value(input[i])).toArray(Value[]::new);
        return new ListValue(values);
    }

    /**
     * Returns a value from long vararg.
     *
     * @param input the long value(s)
     * @return the value
     */
    public static Value value(long... input) {
        var values = Arrays.stream(input).mapToObj(Values::value).toArray(Value[]::new);
        return new ListValue(values);
    }

    /**
     * Returns a value from short vararg.
     *
     * @param input the short value(s)
     * @return the value
     */
    public static Value value(short... input) {
        var values =
                IntStream.range(0, input.length).mapToObj(i -> value(input[i])).toArray(Value[]::new);
        return new ListValue(values);
    }

    /**
     * Returns a value from int vararg.
     *
     * @param input the int value(s)
     * @return the value
     */
    public static Value value(int... input) {
        var values = Arrays.stream(input).mapToObj(Values::value).toArray(Value[]::new);
        return new ListValue(values);
    }

    /**
     * Returns a value from double vararg.
     *
     * @param input the double value(s)
     * @return the value
     */
    public static Value value(double... input) {
        var values = Arrays.stream(input).mapToObj(Values::value).toArray(Value[]::new);
        return new ListValue(values);
    }

    /**
     * Returns a value from float vararg.
     *
     * @param input the float value(s)
     * @return the value
     */
    public static Value value(float... input) {
        var values =
                IntStream.range(0, input.length).mapToObj(i -> value(input[i])).toArray(Value[]::new);
        return new ListValue(values);
    }

    /**
     * Returns a value from list of objects.
     *
     * @param vals the list of objects
     * @return the value
     */
    public static Value value(List<Object> vals) {
        var values = new Value[vals.size()];
        var i = 0;
        for (var val : vals) {
            values[i++] = value(val);
        }
        return new ListValue(values);
    }

    /**
     * Returns a value from iterable of objects.
     *
     * @param val the iterable of objects
     * @return the value
     */
    public static Value value(Iterable<Object> val) {
        return value(val.iterator());
    }

    /**
     * Returns a value from iterator of objects.
     *
     * @param val the iterator of objects
     * @return the value
     */
    public static Value value(Iterator<Object> val) {
        List<Value> values = new ArrayList<>();
        while (val.hasNext()) {
            values.add(value(val.next()));
        }
        return new ListValue(values.toArray(new Value[0]));
    }

    /**
     * Returns a value from stream of objects.
     *
     * @param stream the stream of objects
     * @return the value
     */
    public static Value value(Stream<Object> stream) {
        var values = stream.map(Values::value).toArray(Value[]::new);
        return new ListValue(values);
    }

    /**
     * Returns a value from char.
     *
     * @param val the char value
     * @return the value
     */
    public static Value value(final char val) {
        return new StringValue(String.valueOf(val));
    }

    /**
     * Returns a value from string.
     *
     * @param val the string value
     * @return the value
     */
    public static Value value(final String val) {
        return new StringValue(val);
    }

    /**
     * Returns a value from long.
     *
     * @param val the long value
     * @return the value
     */
    public static Value value(final long val) {
        return new IntegerValue(val);
    }

    /**
     * Returns a value from int.
     *
     * @param val the int value
     * @return the value
     */
    public static Value value(final int val) {
        return new IntegerValue(val);
    }

    /**
     * Returns a value from double.
     *
     * @param val the double value
     * @return the value
     */
    public static Value value(final double val) {
        return new FloatValue(val);
    }

    /**
     * Returns a value from boolean.
     *
     * @param val the boolean value
     * @return the value
     */
    public static Value value(final boolean val) {
        return BooleanValue.fromBoolean(val);
    }

    /**
     * Returns a value from string to object map.
     *
     * @param val the string to object map
     * @return the value
     */
    public static Value value(final Map<String, Object> val) {
        Map<String, Value> asValues = new HashMap<>(val.size());
        for (var entry : val.entrySet()) {
            asValues.put(entry.getKey(), value(entry.getValue()));
        }
        return new MapValue(asValues);
    }

    /**
     * Returns a value from local date.
     *
     * @param localDate the local date value
     * @return the value
     */
    public static Value value(LocalDate localDate) {
        return new DateValue(localDate);
    }

    /**
     * Returns a value from offset time.
     *
     * @param offsetTime the offset time value
     * @return the value
     */
    public static Value value(OffsetTime offsetTime) {
        return new TimeValue(offsetTime);
    }

    /**
     * Returns a value from local time.
     *
     * @param localTime the local time value
     * @return the value
     */
    public static Value value(LocalTime localTime) {
        return new LocalTimeValue(localTime);
    }

    /**
     * Returns a value from local date time.
     *
     * @param localDateTime the local date time value
     * @return the value
     */
    public static Value value(LocalDateTime localDateTime) {
        return new LocalDateTimeValue(localDateTime);
    }

    /**
     * Returns a value from offset date time.
     *
     * @param offsetDateTime the offset date time value
     * @return the value
     */
    public static Value value(OffsetDateTime offsetDateTime) {
        return new DateTimeValue(offsetDateTime.toZonedDateTime());
    }

    /**
     * Returns a value from zoned date time.
     *
     * @param zonedDateTime the zoned date time value
     * @return the value
     */
    public static Value value(ZonedDateTime zonedDateTime) {
        return new DateTimeValue(zonedDateTime);
    }

    /**
     * Returns a value from period.
     *
     * @param period the period value
     * @return the value
     */
    public static Value value(Period period) {
        return value(new InternalIsoDuration(period));
    }

    /**
     * Returns a value from duration.
     *
     * @param duration the duration value
     * @return the value
     */
    public static Value value(Duration duration) {
        return value(new InternalIsoDuration(duration));
    }

    /**
     * Returns a value from month, day, seconds and nanoseconds values.
     *
     * @param months      the month value
     * @param days        the day value
     * @param seconds     the seconds value
     * @param nanoseconds the nanoseconds value
     * @return the value
     */
    public static Value isoDuration(long months, long days, long seconds, int nanoseconds) {
        return value(new InternalIsoDuration(months, days, seconds, nanoseconds));
    }

    /**
     * Returns a value from ISO duration.
     *
     * @param duration the ISO duration value
     * @return the value
     */
    private static Value value(IsoDuration duration) {
        return new DurationValue(duration);
    }

    /**
     * Returns a value from SRID, x and y values.
     *
     * @param srid the SRID value
     * @param x    the x value
     * @param y    the y value
     * @return the value
     */
    public static Value point(int srid, double x, double y) {
        return value(new InternalPoint2D(srid, x, y));
    }

    /**
     * Returns a value from point.
     *
     * @param point the point value
     * @return the value
     */
    private static Value value(Point point) {
        return new PointValue(point);
    }

    /**
     * Returns a value from SRID, x ,y and z values.
     *
     * @param srid the SRID value
     * @param x    the x value
     * @param y    the y value
     * @param z    the z value
     * @return the value
     */
    public static Value point(int srid, double x, double y, double z) {
        return value(new InternalPoint3D(srid, x, y, z));
    }
}
