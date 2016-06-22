/**
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
package org.neo4j.driver.internal.pool;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.neo4j.driver.internal.connector.socket.SocketConnector;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.Connector;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.Neo4jException;

import static java.lang.String.format;

/**
 * A basic connection pool that optimizes for threads being long-lived, acquiring/releasing many connections.
 * It uses a global queue as a fallback pool, but tries to avoid coordination by storing connections in a ThreadLocal.
 *
 * Safety is achieved by tracking thread locals getting garbage collected, returning connections to the global pool
 * when this happens.
 *
 * If threads are long-lived, this pool will achieve linearly scalable performance with overhead equivalent to a
 * hash-map lookup per acquire.
 *
 * If threads are short-lived, this pool is not ideal.
 */
public class InternalConnectionPool implements ConnectionPool
{
    /**
     * Map of scheme -> connector, this is what we use to establish new connections.
     */
    private final ConcurrentHashMap<String,Connector> connectors = new ConcurrentHashMap<>();

    /**
     * Pools, organized by URL.
     */
    private final ConcurrentHashMap<URI,BlockingQueue<PooledConnection>> pools = new ConcurrentHashMap<>();

    private final AuthToken authToken;
    private final Clock clock;
    private final Config config;

    public InternalConnectionPool( Config config, AuthToken authToken )
    {
        this( loadConnectors(), Clock.SYSTEM, config, authToken);
    }

    public InternalConnectionPool( Collection<Connector> conns, Clock clock, Config config,
            AuthToken authToken )
    {
        this.authToken = authToken;
        this.config = config;
        this.clock = clock;
        for ( Connector connector : conns )
        {
            for ( String s : connector.supportedSchemes() )
            {
                this.connectors.put( s, connector );
            }
        }
    }

    @Override
    public Connection acquire( URI sessionURI )
    {
            BlockingQueue<PooledConnection> connections = pool( sessionURI );
            PooledConnection conn = connections.poll();
            if ( conn == null )
            {
                Connector connector = connectors.get( sessionURI.getScheme() );
                if ( connector == null )
                {
                    throw new ClientException(
                            format( "Unsupported URI scheme: '%s' in url: '%s'. Supported transports are: '%s'.",
                                    sessionURI.getScheme(), sessionURI, connectorSchemes() ) );
                }
                conn = new PooledConnection(connector.connect( sessionURI, config, authToken ), new PooledConnectionReleaseConsumer( connections, config ), clock);
            }
            conn.updateUsageTimestamp();
            return conn;
    }

    private BlockingQueue<PooledConnection> pool( URI sessionURI )
    {
        BlockingQueue<PooledConnection> pool = pools.get( sessionURI );
        if ( pool == null )
        {
            pool = new LinkedBlockingQueue<>(config.maxIdleConnectionPoolSize());
            if ( pools.putIfAbsent( sessionURI, pool ) != null )
            {
                // We lost a race to create the pool, dispose of the one we created, and recurse
                return pool( sessionURI );
            }
        }
        return pool;
    }

    private static Collection<Connector> loadConnectors()
    {
        List<Connector> connectors = new LinkedList<>();

        // Hard code socket connector
        Connector conn = new SocketConnector();
        connectors.add( conn );

        // Load custom loadConnectors via JSL
        ServiceLoader<Connector> load = ServiceLoader.load( Connector.class );
        for ( Connector connector : load )
        {
            connectors.add( connector );
        }
        return connectors;
    }

    @Override
    public void close() throws Neo4jException
    {
        for ( BlockingQueue<PooledConnection> pool : pools.values() )
        {
            while ( !pool.isEmpty() )
            {
                PooledConnection conn = pool.poll();
                if ( conn != null )
                {
                    //close the underlying connection without adding it back to the queue
                    conn.dispose();
                }
            }
        }

        pools.clear();
    }

    private String connectorSchemes()
    {
        return Arrays.toString( connectors.keySet().toArray( new String[connectors.keySet().size()] ) );
    }
}
