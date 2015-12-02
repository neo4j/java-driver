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
package org.neo4j.driver.v1.integration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.driver.v1.ImmutableRecord;
import org.neo4j.driver.v1.Result;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.neo4j.driver.v1.Values.parameters;

public class ParametersIT
{
    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldBeAbleToSetAndReturnBooleanProperty()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", true ) );

        // Then
        for ( ImmutableRecord record : result.retain() )
        {
            Value value = record.value( "a.value" );
            assertThat( value.hasType( session.typeSystem().BOOLEAN() ), equalTo( true ) );
            assertThat( value.asBoolean(), equalTo( true ) );
        }
    }

    @Test
    public void shouldBeAbleToSetAndReturnByteProperty()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", (byte) 1 ) );

        // Then
        for ( ImmutableRecord record : result.retain() )
        {
            Value value = record.value( "a.value" );
            assertThat( value.hasType( session.typeSystem().INTEGER() ), equalTo( true ) );
            assertThat( value.asLong(), equalTo( 1L ) );
        }
    }

    @Test
    public void shouldBeAbleToSetAndReturnShortProperty()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", (short) 1 ) );

        // Then
        for ( ImmutableRecord record : result.retain() )
        {
            Value value = record.value( "a.value" );
            assertThat( value.hasType( session.typeSystem().INTEGER() ), equalTo( true ) );
            assertThat( value.asLong(), equalTo( 1L ) );
        }
    }

    @Test
    public void shouldBeAbleToSetAndReturnIntegerProperty()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", 1 ) );

        // Then
        for ( ImmutableRecord record : result.retain() )
        {
            Value value = record.value( "a.value" );
            assertThat( value.hasType( session.typeSystem().INTEGER() ), equalTo( true ) );
            assertThat( value.asLong(), equalTo( 1L ) );
        }

    }

    @Test
    public void shouldBeAbleToSetAndReturnLongProperty()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", 1L ) );

        // Then
        for ( ImmutableRecord record : result.retain() )
        {
            Value value = record.value( "a.value" );
            assertThat( value.hasType( session.typeSystem().INTEGER() ), equalTo( true ) );
            assertThat( value.asLong(), equalTo( 1L ) );
        }

    }

    @Test
    public void shouldBeAbleToSetAndReturnDoubleProperty()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", 6.28 ) );

        // Then
        for ( ImmutableRecord record : result.retain() )
        {
            Value value = record.value( "a.value" );
            assertThat( value.hasType( session.typeSystem().FLOAT() ), equalTo( true ) );
            assertThat( value.asDouble(), equalTo( 6.28 ) );
        }

    }

    @Test
    public void shouldBeAbleToSetAndReturnCharacterProperty()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", 'ö' ) );

        // Then
        for ( ImmutableRecord record : result.retain() )
        {
            Value value = record.value( "a.value" );
            assertThat( value.hasType( session.typeSystem().STRING() ), equalTo( true ) );
            assertThat( value.asString(), equalTo( "ö" ) );
        }

    }

    @Test
    public void shouldBeAbleToSetAndReturnCharacterArrayProperty()
    {
        // When
        char[] arrayValue = new char[]{'M', 'j', 'ö', 'l', 'n', 'i', 'r'};
        Result result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", arrayValue ) );

        // Then
        for ( ImmutableRecord record : result.retain() )
        {
            Value value = record.value( "a.value" );
            assertThat( value.hasType( session.typeSystem().STRING() ), equalTo( true ) );
            assertThat( value.asString(), equalTo( "Mjölnir" ) );
        }

    }

    @Test
    public void shouldBeAbleToSetAndReturnStringProperty()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", "Mjölnir" ) );

        // Then
        for ( ImmutableRecord record : result.retain() )
        {
            Value value = record.value( "a.value" );
            assertThat( value.hasType( session.typeSystem().STRING() ), equalTo( true ) );
            assertThat( value.asString(), equalTo( "Mjölnir" ) );
        }

    }

    @Test
    public void shouldBeAbleToSetAndReturnBooleanArrayProperty()
    {
        // When
        boolean[] arrayValue = new boolean[]{true, true, true};
        Result result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", arrayValue ) );

        // Then
        for ( ImmutableRecord record : result.retain() )
        {
            Value value = record.value( "a.value" );
            assertThat( value.hasType( session.typeSystem().LIST() ), equalTo( true ) );
            assertThat( value.elementCount(), equalTo( 3 ) );
            for ( Value item : value.asList() )
            {
                assertThat( item.hasType( session.typeSystem().BOOLEAN() ), equalTo( true ) );
                assertThat( item.asBoolean(), equalTo( true ) );
            }
        }

    }

    @Test
    public void shouldBeAbleToSetAndReturnIntegerArrayProperty()
    {
        // When
        int[] arrayValue = new int[]{42, 42, 42};
        Result result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", arrayValue ) );

        // Then
        for ( ImmutableRecord record : result.retain() )
        {
            Value value = record.value( "a.value" );
            assertThat( value.hasType( session.typeSystem().LIST() ), equalTo( true ) );
            assertThat( value.elementCount(), equalTo( 3 ) );
            for ( Value item : value.asList() )
            {
                assertThat( item.hasType( session.typeSystem().INTEGER() ), equalTo( true ) );
                assertThat( item.asLong(), equalTo( 42L ) );
            }
        }

    }

    @Test
    public void shouldBeAbleToSetAndReturnDoubleArrayProperty()
    {
        // When
        double[] arrayValue = new double[]{6.28, 6.28, 6.28};
        Result result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", arrayValue ) );

        // Then
        for ( ImmutableRecord record : result.retain() )
        {
            Value value = record.value( "a.value" );
            assertThat( value.hasType( session.typeSystem().LIST() ), equalTo( true ) );
            assertThat( value.elementCount(), equalTo( 3 ) );
            for ( Value item : value.asList() )
            {
                assertThat( item.hasType( session.typeSystem().FLOAT() ), equalTo( true ) );
                assertThat( item.asDouble(), equalTo( 6.28 ) );
            }
        }
    }

    @Test
    public void shouldBeAbleToSetAndReturnSpecialStringArrayProperty()
    {
        // When
        String[] arrayValue = new String[]{"Mjölnir", "Mjölnir", "Mjölnir"};

        Result result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", arrayValue ) );

        // Then
        for ( ImmutableRecord record : result.retain() )
        {
            Value value = record.value( "a.value" );
            assertThat( value.hasType( session.typeSystem().LIST() ), equalTo( true ) );
            assertThat( value.elementCount(), equalTo( 3 ) );
            for ( Value item : value.asList() )
            {
                assertThat( item.hasType( session.typeSystem().STRING() ), equalTo( true ) );
                assertThat( item.asString(), equalTo( "Mjölnir" ) );
            }
        }
    }

    @Test
    public void shouldBeAbleToSetAndReturnStringArrayProperty()
    {
        // When
        String[] arrayValue = new String[]{"cat", "cat", "cat"};
        Result result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", arrayValue ) );

        // Then
        for ( ImmutableRecord record : result.retain() )
        {
            Value value = record.value( "a.value" );
            assertThat( value.hasType( session.typeSystem().LIST() ), equalTo( true ) );
            assertThat( value.elementCount(), equalTo( 3 ) );
            for ( Value item : value.asList() )
            {
                assertThat( item.hasType( session.typeSystem().STRING() ), equalTo( true ) );
                assertThat( item.asString(), equalTo( "cat" ) );
            }
        }

    }

    @Test
    public void shouldBeAbleToSetAndReturnBooleanPropertyWithinMap()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:{value}.v}) RETURN a.value",
                parameters( "value", parameters( "v", true ) ) );

        // Then
        for ( ImmutableRecord record : result.retain() )
        {
            Value value = record.value( "a.value" );
            assertThat( value.hasType( session.typeSystem().BOOLEAN() ), equalTo( true ) );
            assertThat( value.asBoolean(), equalTo( true ) );
        }

    }

    @Test
    public void shouldBeAbleToSetAndReturnIntegerPropertyWithinMap()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:{value}.v}) RETURN a.value",
                parameters( "value", parameters( "v", 42 ) ) );

        // Then
        for ( ImmutableRecord record : result.retain() )
        {
            Value value = record.value( "a.value" );
            assertThat( value.hasType( session.typeSystem().INTEGER() ), equalTo( true ) );
            assertThat( value.asLong(), equalTo( 42L ) );
        }

    }

    @Test
    public void shouldBeAbleToSetAndReturnDoublePropertyWithinMap()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:{value}.v}) RETURN a.value",
                parameters( "value", parameters( "v", 6.28 ) ) );

        // Then
        for ( ImmutableRecord record : result.retain() )
        {
            Value value = record.value( "a.value" );
            assertThat( value.hasType( session.typeSystem().FLOAT() ), equalTo( true ) );
            assertThat( value.asDouble(), equalTo( 6.28 ) );
        }

    }

    @Test
    public void shouldBeAbleToSetAndReturnStringPropertyWithinMap()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:{value}.v}) RETURN a.value",
                parameters( "value", parameters( "v", "Mjölnir" ) ) );

        // Then
        for ( ImmutableRecord record : result.retain() )
        {
            Value value = record.value( "a.value" );
            assertThat( value.hasType( session.typeSystem().STRING() ), equalTo( true ) );
            assertThat( value.asString(), equalTo( "Mjölnir" ) );
        }
    }

    @Test
    public void settingInvalidParameterTypeShouldThrowHelpfulError() throws Throwable
    {
        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Unable to convert java.lang.Object to Neo4j Value." );

        // When
        session.run( "anything", parameters( "k", new Object() ) );
    }
}
