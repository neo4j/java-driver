/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.driver.internal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.internal.async.DecoratedConnection;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;
import static org.neo4j.driver.util.TestUtil.await;

class DirectConnectionProviderTest
{
    @Test
    void acquiresConnectionsFromThePool()
    {
        BoltServerAddress address = BoltServerAddress.LOCAL_DEFAULT;
        Connection connection1 = mock( Connection.class );
        Connection connection2 = mock( Connection.class );

        ConnectionPool pool = poolMock( address, connection1, connection2 );
        DirectConnectionProvider provider = new DirectConnectionProvider( address, pool );

        Connection acquired1 = await( provider.acquireConnection( READ, ABSENT_DB_NAME ) );
        assertThat( acquired1, instanceOf( DecoratedConnection.class ) );
        assertSame( connection1, ((DecoratedConnection) acquired1).connection() );

        Connection acquired2 = await( provider.acquireConnection( WRITE, ABSENT_DB_NAME ) );
        assertThat( acquired2, instanceOf( DecoratedConnection.class ) );
        assertSame( connection2, ((DecoratedConnection) acquired2).connection() );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void returnsCorrectAccessMode( AccessMode mode )
    {
        BoltServerAddress address = BoltServerAddress.LOCAL_DEFAULT;
        ConnectionPool pool = poolMock( address, mock( Connection.class ) );
        DirectConnectionProvider provider = new DirectConnectionProvider( address, pool );

        Connection acquired = await( provider.acquireConnection( mode, ABSENT_DB_NAME ) );

        assertEquals( mode, acquired.mode() );
    }

    @Test
    void closesPool()
    {
        BoltServerAddress address = BoltServerAddress.LOCAL_DEFAULT;
        ConnectionPool pool = poolMock( address, mock( Connection.class ) );
        DirectConnectionProvider provider = new DirectConnectionProvider( address, pool );

        provider.close();

        verify( pool ).close();
    }

    @Test
    void returnsCorrectAddress()
    {
        BoltServerAddress address = new BoltServerAddress( "server-1", 25000 );

        DirectConnectionProvider provider = new DirectConnectionProvider( address, mock( ConnectionPool.class ) );

        assertEquals( address, provider.getAddress() );
    }

    @Test
    void shouldIgnoreDatabaseNameAndAccessModeWhenObtainConnectionFromPool() throws Throwable
    {
        BoltServerAddress address = BoltServerAddress.LOCAL_DEFAULT;
        Connection connection = mock( Connection.class );

        ConnectionPool pool = poolMock( address, connection );
        DirectConnectionProvider provider = new DirectConnectionProvider( address, pool );

        Connection acquired1 = await( provider.acquireConnection( READ, ABSENT_DB_NAME ) );
        assertThat( acquired1, instanceOf( DecoratedConnection.class ) );
        assertSame( connection, ((DecoratedConnection) acquired1).connection() );

        verify( pool ).acquire( address );
    }


    @ParameterizedTest
    @ValueSource( strings = {"", "foo", "data", ABSENT_DB_NAME} )
    void shouldObtainDatabaseNameOnConnection( String databaseName ) throws Throwable
    {
        BoltServerAddress address = BoltServerAddress.LOCAL_DEFAULT;
        ConnectionPool pool = poolMock( address, mock( Connection.class ) );
        DirectConnectionProvider provider = new DirectConnectionProvider( address, pool );

        Connection acquired = await( provider.acquireConnection( READ, databaseName ) );

        assertEquals( databaseName, acquired.databaseName() );
    }

    @SuppressWarnings( "unchecked" )
    private static ConnectionPool poolMock( BoltServerAddress address, Connection connection,
            Connection... otherConnections )
    {
        ConnectionPool pool = mock( ConnectionPool.class );
        CompletableFuture<Connection>[] otherConnectionFutures = Stream.of( otherConnections )
                .map( CompletableFuture::completedFuture )
                .toArray( CompletableFuture[]::new );
        when( pool.acquire( address ) ).thenReturn( completedFuture( connection ), otherConnectionFutures );
        return pool;
    }
}
