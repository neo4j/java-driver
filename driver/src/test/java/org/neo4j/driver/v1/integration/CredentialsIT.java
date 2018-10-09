/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Map;

import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.AuthenticationException;
import org.neo4j.driver.v1.exceptions.SecurityException;
import org.neo4j.driver.v1.util.DatabaseExtension;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.v1.AuthTokens.basic;
import static org.neo4j.driver.v1.AuthTokens.custom;
import static org.neo4j.driver.v1.Values.ofValue;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.util.Neo4jRunner.PASSWORD;

class CredentialsIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @Test
    void shouldBePossibleToChangePassword() throws Exception
    {
        String newPassword = "secret";
        // change the password
        changePassword( PASSWORD, newPassword );

        try
        {
            // verify old password does not work
            assertThrows( AuthenticationException.class, () -> GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", PASSWORD ) ) );

            // verify new password works
            try ( Driver driver = GraphDatabase.driver( neo4j.uri(), AuthTokens.basic( "neo4j", newPassword ) ); Session session = driver.session() )
            {
                session.run( "RETURN 2" ).consume();
            }
        }
        finally
        {
            // change back the password
            changePassword( newPassword, PASSWORD );
        }
    }

    @Test
    void basicCredentialsShouldWork()
    {
        // When & Then
        try( Driver driver = GraphDatabase.driver( neo4j.uri(),
                basic( "neo4j", PASSWORD ) );
             Session session = driver.session() )
        {
            Value single = session.run( "RETURN 1" ).single().get( 0 );
            assertThat( single.asLong(), equalTo( 1L ) );
        }
    }

    @Test
    void shouldGetHelpfulErrorOnInvalidCredentials()
    {
        SecurityException e = assertThrows( SecurityException.class, () ->
        {
            try ( Driver driver = GraphDatabase.driver( neo4j.uri(), basic( "thisisnotthepassword", PASSWORD ) );
                  Session session = driver.session() )
            {
                session.run( "RETURN 1" );
            }
        } );
        assertThat( e.getMessage(), containsString( "The client is unauthorized due to authentication failure." ) );
    }

    @Test
    void shouldBeAbleToProvideRealmWithBasicAuth()
    {
        // When & Then
        try( Driver driver = GraphDatabase.driver( neo4j.uri(),
                basic( "neo4j", PASSWORD, "native" ) );
             Session session = driver.session() )
        {
            Value single = session.run( "CREATE () RETURN 1" ).single().get( 0 );
            assertThat( single.asLong(), equalTo( 1L ) );
        }
    }

    @Test
    void shouldBeAbleToConnectWithCustomToken()
    {
        // When & Then
        try( Driver driver = GraphDatabase.driver( neo4j.uri(),
                custom( "neo4j", PASSWORD, "native", "basic" ) );
             Session session = driver.session() )
        {
            Value single = session.run( "CREATE () RETURN 1" ).single().get( 0 );
            assertThat( single.asLong(), equalTo( 1L ) );
        }
    }

    @Test
    void shouldBeAbleToConnectWithCustomTokenWithAdditionalParameters()
    {
        Map<String,Object> params = singletonMap( "secret", 16 );

        // When & Then
        try( Driver driver = GraphDatabase.driver( neo4j.uri(),
                custom( "neo4j", PASSWORD, "native", "basic", params ) );
             Session session = driver.session() )
        {
            Value single = session.run( "CREATE () RETURN 1" ).single().get( 0 );
            assertThat( single.asLong(), equalTo( 1L ) );
        }
    }

    @Test
    void directDriverShouldFailEarlyOnWrongCredentials()
    {
        testDriverFailureOnWrongCredentials( "bolt://localhost" );
    }

    @Test
    void routingDriverShouldFailEarlyOnWrongCredentials()
    {
        testDriverFailureOnWrongCredentials( "bolt+routing://localhost" );
    }

    private void testDriverFailureOnWrongCredentials( String uri )
    {
        Config config = Config.build().withLogging( DEV_NULL_LOGGING ).toConfig();
        AuthToken authToken = AuthTokens.basic( "neo4j", "wrongSecret" );

        assertThrows( AuthenticationException.class, () -> GraphDatabase.driver( uri, authToken, config ) );
    }

    private static void changePassword( String oldSecret, String newSecret )
    {
        AuthToken authToken = new InternalAuthToken( parameters(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", oldSecret,
                "new_credentials", newSecret ).asMap( ofValue() ) );

        // change the password
        try ( Driver driver = GraphDatabase.driver( neo4j.uri(), authToken );
                Session session = driver.session() )
        {
            session.run( "RETURN 1" ).consume();
        }
    }
}
