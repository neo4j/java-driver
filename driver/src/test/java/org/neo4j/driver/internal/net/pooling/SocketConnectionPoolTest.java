/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.driver.internal.net.pooling;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.Connector;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.v1.Logging;

import static java.util.Collections.newSetFromMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.net.BoltServerAddress.DEFAULT_PORT;
import static org.neo4j.driver.internal.net.BoltServerAddress.LOCAL_DEFAULT;

public class SocketConnectionPoolTest
{
    private static final BoltServerAddress ADDRESS_1 = LOCAL_DEFAULT;
    private static final BoltServerAddress ADDRESS_2 = new BoltServerAddress( "localhost", DEFAULT_PORT + 42 );
    private static final BoltServerAddress ADDRESS_3 = new BoltServerAddress( "localhost", DEFAULT_PORT + 4242 );

    @Test
    public void acquireCreatesNewConnectionWhenPoolIsEmpty() throws Throwable
    {
        Connector connector = newMockConnector();
        SocketConnectionPool pool = newPool( connector );

        Connection connection = pool.acquire( ADDRESS_1 );

        assertThat( connection, instanceOf( PooledConnection.class ) );
        verify( connector ).connect( ADDRESS_1 );
    }

    @Test
    public void acquireUsesExistingConnectionIfPresent() throws Throwable
    {
        Connection connection = newConnectionMock( ADDRESS_1 );
        Connector connector = newMockConnector( connection );

        SocketConnectionPool pool = newPool( connector );

        Connection acquiredConnection1 = pool.acquire( ADDRESS_1 );
        assertThat( acquiredConnection1, instanceOf( PooledConnection.class ) );
        acquiredConnection1.close(); // return connection to the pool

        Connection acquiredConnection2 = pool.acquire( ADDRESS_1 );
        assertThat( acquiredConnection2, instanceOf( PooledConnection.class ) );

        verify( connector ).connect( ADDRESS_1 );
    }

    @Test
    public void purgeDoesNothingForNonExistingAddress() throws Throwable
    {
        Connection connection = newConnectionMock( ADDRESS_1 );
        SocketConnectionPool pool = newPool( newMockConnector( connection ) );

        pool.acquire( ADDRESS_1 ).close();

        assertTrue( pool.hasAddress( ADDRESS_1 ) );
        pool.purge( ADDRESS_2 );
        assertTrue( pool.hasAddress( ADDRESS_1 ) );
    }

    @Test
    public void purgeRemovesAddress() throws Throwable
    {
        Connection connection = newConnectionMock( ADDRESS_1 );
        SocketConnectionPool pool = newPool( newMockConnector( connection ) );

        pool.acquire( ADDRESS_1 ).close();

        assertTrue( pool.hasAddress( ADDRESS_1 ) );
        pool.purge( ADDRESS_1 );
        assertFalse( pool.hasAddress( ADDRESS_1 ) );
    }

    @Test
    public void purgeTerminatesPoolCorrespondingToTheAddress() throws Throwable
    {
        Connection connection1 = newConnectionMock( ADDRESS_1 );
        Connection connection2 = newConnectionMock( ADDRESS_1 );
        Connection connection3 = newConnectionMock( ADDRESS_1 );
        SocketConnectionPool pool = newPool( newMockConnector( connection1, connection2, connection3 ) );

        Connection pooledConnection1 = pool.acquire( ADDRESS_1 );
        Connection pooledConnection2 = pool.acquire( ADDRESS_1 );
        pool.acquire( ADDRESS_1 );

        // return two connections to the pool
        pooledConnection1.close();
        pooledConnection2.close();

        pool.purge( ADDRESS_1 );

        verify( connection1 ).close();
        verify( connection2 ).close();
        verify( connection3 ).close();
    }

    @Test
    public void hasAddressReturnsFalseWhenPoolIsEmpty() throws Throwable
    {
        SocketConnectionPool pool = newPool( newMockConnector() );

        assertFalse( pool.hasAddress( ADDRESS_1 ) );
        assertFalse( pool.hasAddress( ADDRESS_2 ) );
    }

    @Test
    public void hasAddressReturnsFalseForUnknownAddress() throws Throwable
    {
        SocketConnectionPool pool = newPool( newMockConnector() );

        assertNotNull( pool.acquire( ADDRESS_1 ) );

        assertFalse( pool.hasAddress( ADDRESS_2 ) );
    }

    @Test
    public void hasAddressReturnsTrueForKnownAddress() throws Throwable
    {
        SocketConnectionPool pool = newPool( newMockConnector() );

        assertNotNull( pool.acquire( ADDRESS_1 ) );

        assertTrue( pool.hasAddress( ADDRESS_1 ) );
    }

    @Test
    public void closeTerminatesAllPools() throws Throwable
    {
        Connection connection1 = newConnectionMock( ADDRESS_1 );
        Connection connection2 = newConnectionMock( ADDRESS_1 );
        Connection connection3 = newConnectionMock( ADDRESS_2 );
        Connection connection4 = newConnectionMock( ADDRESS_2 );

        Connector connector = newMockConnector( connection1, connection2, connection3, connection4 );

        SocketConnectionPool pool = newPool( connector );

        assertNotNull( pool.acquire( ADDRESS_1 ) );
        pool.acquire( ADDRESS_1 ).close(); // return to the pool
        assertNotNull( pool.acquire( ADDRESS_2 ) );
        pool.acquire( ADDRESS_2 ).close(); // return to the pool

        assertTrue( pool.hasAddress( ADDRESS_1 ) );
        assertTrue( pool.hasAddress( ADDRESS_2 ) );

        pool.close();

        verify( connection1 ).close();
        verify( connection2 ).close();
        verify( connection3 ).close();
        verify( connection4 ).close();
    }

    @Test
    public void closeRemovesAllPools() throws Throwable
    {
        Connection connection1 = newConnectionMock( ADDRESS_1 );
        Connection connection2 = newConnectionMock( ADDRESS_2 );
        Connection connection3 = newConnectionMock( ADDRESS_3 );

        Connector connector = newMockConnector( connection1, connection2, connection3 );

        SocketConnectionPool pool = newPool( connector );

        assertNotNull( pool.acquire( ADDRESS_1 ) );
        assertNotNull( pool.acquire( ADDRESS_2 ) );
        assertNotNull( pool.acquire( ADDRESS_3 ) );

        assertTrue( pool.hasAddress( ADDRESS_1 ) );
        assertTrue( pool.hasAddress( ADDRESS_2 ) );
        assertTrue( pool.hasAddress( ADDRESS_3 ) );

        pool.close();

        assertFalse( pool.hasAddress( ADDRESS_1 ) );
        assertFalse( pool.hasAddress( ADDRESS_2 ) );
        assertFalse( pool.hasAddress( ADDRESS_3 ) );
    }

    @Test
    public void closeWithConcurrentAcquisitionsEmptiesThePool() throws Throwable
    {
        Connector connector = mock( Connector.class );
        Set<Connection> createdConnections = newSetFromMap( new ConcurrentHashMap<Connection,Boolean>() );
        when( connector.connect( any( BoltServerAddress.class ) ) )
                .then( createConnectionAnswer( createdConnections ) );

        SocketConnectionPool pool = newPool( connector );

        ExecutorService executor = Executors.newCachedThreadPool();
        List<Future<Void>> results = new ArrayList<>();

        AtomicInteger port = new AtomicInteger();
        for ( int i = 0; i < 5; i++ )
        {
            Future<Void> result = executor.submit( acquireConnection( pool, port ) );
            results.add( result );
        }

        Thread.sleep( 500 ); // allow workers to do something

        pool.close();

        for ( Future<Void> result : results )
        {
            try
            {
                result.get( 20, TimeUnit.SECONDS );
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( ExecutionException.class ) );
                assertThat( e.getCause(), instanceOf( IllegalStateException.class ) );
            }
        }
        executor.shutdownNow();
        executor.awaitTermination( 10, TimeUnit.SECONDS );

        for ( int i = 0; i < port.intValue(); i++ )
        {
            assertFalse( pool.hasAddress( new BoltServerAddress( "localhost", i ) ) );
        }
        for ( Connection connection : createdConnections )
        {
            verify( connection ).close();
        }
    }

    private static Answer<Connection> createConnectionAnswer( final Set<Connection> createdConnections )
    {
        return new Answer<Connection>()
        {
            @Override
            public Connection answer( InvocationOnMock invocation )
            {
                BoltServerAddress address = invocation.getArgumentAt( 0, BoltServerAddress.class );
                Connection connection = newConnectionMock( address );
                createdConnections.add( connection );
                return connection;
            }
        };
    }

    private static Callable<Void> acquireConnection( final SocketConnectionPool pool, final AtomicInteger port )
    {
        return new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                while ( true )
                {
                    pool.acquire( new BoltServerAddress( "localhost", port.incrementAndGet() ) );
                }
            }
        };
    }

    private static Connector newMockConnector() throws Throwable
    {
        Connection connection = mock( Connection.class );
        return newMockConnector( connection );
    }

    private static Connector newMockConnector( Connection connection, Connection... otherConnections ) throws Throwable
    {
        Connector connector = mock( Connector.class );
        when( connector.connect( any( BoltServerAddress.class ) ) ).thenReturn( connection, otherConnections );
        return connector;
    }

    private static SocketConnectionPool newPool( Connector connector ) throws Throwable
    {
        PoolSettings poolSettings = new PoolSettings( 42 );
        Logging logging = mock( Logging.class, RETURNS_MOCKS );
        return new SocketConnectionPool( poolSettings, connector, logging );
    }

    private static Connection newConnectionMock( BoltServerAddress address )
    {
        Connection connection = mock( Connection.class );
        if ( address != null )
        {
            when( connection.boltServerAddress() ).thenReturn( address );
        }
        return connection;
    }
}
