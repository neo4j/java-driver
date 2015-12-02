/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
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
package org.neo4j.driver.v1.internal;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.internal.value.ListValue;
import org.neo4j.driver.v1.internal.value.MapValue;
import org.neo4j.driver.v1.internal.value.StringValue;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import static org.neo4j.driver.v1.Values.value;
import static org.neo4j.driver.v1.Values.valueAsList;
import static org.neo4j.driver.v1.Values.valueToString;
import static org.neo4j.driver.v1.Values.values;

public class ValuesTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldConvertPrimitiveArrays() throws Throwable
    {
        assertThat( value( new int[]{1, 2, 3} ),
                equalTo( (Value) new ListValue( values( 1, 2, 3 ) ) ) );

        assertThat( value( new short[]{1, 2, 3} ),
                equalTo( (Value) new ListValue( values( 1, 2, 3 ) ) ) );

        assertThat( value( new char[]{'a', 'b', 'c'} ),
                equalTo( (Value) new StringValue( "abc" ) ) );

        assertThat( value( new long[]{1, 2, 3} ),
                equalTo( (Value) new ListValue( values( 1, 2, 3 ) ) ) );

        assertThat( value( new float[]{1.1f, 2.2f, 3.3f} ),
                equalTo( (Value) new ListValue( values( 1.1f, 2.2f, 3.3f ) ) ) );

        assertThat( value( new double[]{1.1, 2.2, 3.3} ),
                equalTo( (Value) new ListValue( values( 1.1, 2.2, 3.3 ) ) ) );

        assertThat( value( new boolean[]{true, false, true} ),
                equalTo( (Value) new ListValue( values( true, false, true ) ) ) );

        assertThat( value( new String[]{"a", "b", "c"} ),
                equalTo( (Value) new ListValue( values( "a", "b", "c" ) ) ) );
    }

    @Test
    public void shouldComplainAboutStrangeTypes() throws Throwable
    {
        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Unable to convert java.lang.Object to Neo4j Value." );

        // When
        value( new Object() );
    }

    @Test
    public void equalityRules() throws Throwable
    {
        assertEquals( value( 1 ), value( 1 ) );
        assertEquals( value( Long.MAX_VALUE ), value( Long.MAX_VALUE ) );
        assertEquals( value( Long.MIN_VALUE ), value( Long.MIN_VALUE ) );
        assertNotEquals( value( 1 ), value( 2 ) );

        assertEquals( value( 1.1337 ), value( 1.1337 ) );
        assertEquals( value( Double.MAX_VALUE ), value( Double.MAX_VALUE ) );
        assertEquals( value( Double.MIN_VALUE ), value( Double.MIN_VALUE ) );

        assertEquals( value( true ), value( true ) );
        assertEquals( value( false ), value( false ) );
        assertNotEquals( value( true ), value( false ) );

        assertEquals( value( "Hello" ), value( "Hello" ) );
        assertEquals( value( "This åäö string ?? contains strange Ü" ),
                value( "This åäö string ?? contains strange Ü" ) );
        assertEquals( value( "" ), value( "" ) );
        assertNotEquals( value( "Hello" ), value( "hello" ) );
        assertNotEquals( value( "This åäö string ?? contains strange " ),
                value( "This åäö string ?? contains strange Ü" ) );
    }

    @Test
    public void shouldMapDriverComplexTypesToListOfJavaPrimitiveTypes() throws Throwable
    {
        // Given
        Map<String,Value> map = new HashMap<>();
        map.put( "Cat", new ListValue( values( "meow", "miaow" ) ) );
        map.put( "Dog", new ListValue( values( "wow" ) ) );
        map.put( "Wrong", new ListValue( values( -1 ) ) );
        MapValue values = new MapValue( map );

        // When
        Iterable<List<String>> list = values.values( valueAsList( valueToString() ) );

        // Then
        assertEquals( 3, values.countElements() );
        Iterator<List<String>> listIterator = list.iterator();
        for ( Value value : values.values() )
        {
            assertEquals( value.value( 0 ).toString(), listIterator.next().get( 0 ) );
        }
    }

    @Test
    public void shouldMapDriverMapsToJavaMaps() throws Throwable
    {
        // Given
        Map<String,Value> map = new HashMap<>();
        map.put( "Cat", value( 1 ) );
        map.put( "Dog", value( 2 ) );
        MapValue values = new MapValue( map );

        // When
        Map<String, String> result = values.asMap( Values.valueToString() );

        // Then
        assertThat( result.size(), equalTo( 2 ) );
        assertThat( result.get( "Dog" ), equalTo( "integer<2>" ) );
        assertThat( result.get( "Cat" ), equalTo( "integer<1>" ) );
    }

    @Test
    public void convertIntegerValue()
    {
        // GIVEN
        Value value = value( 42 );

        // THEN
        assertThat(value.asByte(), equalTo((byte) 42));
        assertThat(value.asShort(), equalTo((short) 42));
        assertThat(value.asInt(), equalTo( 42));
        assertThat(value.asLong(), equalTo( 42L));
        assertThat(value.asDouble(), equalTo( 42D));
        assertThat(value.asFloat(), equalTo( 42F));


    }
}
