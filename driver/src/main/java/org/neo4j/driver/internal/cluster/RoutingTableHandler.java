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
package org.neo4j.driver.internal.cluster;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Logger;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.RoutingErrorHandler;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Futures;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class RoutingTableHandler implements RoutingErrorHandler
{
    private final RoutingTable routingTable;
    private final RoutingTables routingTables;
    private CompletableFuture<RoutingTable> refreshRoutingTableFuture;
    private final ConnectionPool connectionPool;
    private final Rediscovery rediscovery;
    private final Logger log;

    public RoutingTableHandler( RoutingTable routingTable, Rediscovery rediscovery, ConnectionPool connectionPool, RoutingTables routingTables, Logger log )
    {
        this.routingTable = routingTable;
        this.rediscovery = rediscovery;
        this.connectionPool = connectionPool;
        this.routingTables = routingTables;
        this.log = log;
    }

    @Override
    public void onConnectionFailure( BoltServerAddress address )
    {
        // remove from the routing table, to prevent concurrent threads from making connections to this address
        routingTable.forget( address );
    }

    @Override
    public void onWriteFailure( BoltServerAddress address )
    {
        routingTable.removeWriter( address );
    }

    public synchronized CompletionStage<RoutingTable> freshRoutingTable( AccessMode mode )
    {
        if ( refreshRoutingTableFuture != null )
        {
            // refresh is already happening concurrently, just use it's result
            return refreshRoutingTableFuture;
        }
        else if ( routingTable.isStaleFor( mode ) )
        {
            // existing routing table is not fresh and should be updated
            log.info( "Routing table is stale. %s", routingTable );

            CompletableFuture<RoutingTable> resultFuture = new CompletableFuture<>();
            refreshRoutingTableFuture = resultFuture;

            rediscovery.lookupClusterComposition( routingTable, connectionPool )
                    .whenComplete( ( composition, completionError ) ->
                    {
                        Throwable error = Futures.completionExceptionCause( completionError );
                        if ( error != null )
                        {
                            clusterCompositionLookupFailed( error );
                        }
                        else
                        {
                            freshClusterCompositionFetched( composition );
                        }
                    } );

            return resultFuture;
        }
        else
        {
            // existing routing table is fresh, use it
            return completedFuture( routingTable );
        }
    }

    private synchronized void freshClusterCompositionFetched( ClusterComposition composition )
    {
        try
        {
            routingTable.update( composition );
            connectionPool.retainAll( routingTables.allServers() );

            log.info( "Updated routing table. %s", routingTable );

            CompletableFuture<RoutingTable> routingTableFuture = refreshRoutingTableFuture;
            refreshRoutingTableFuture = null;
            routingTableFuture.complete( routingTable );
        }
        catch ( Throwable error )
        {
            clusterCompositionLookupFailed( error );
        }
    }

    private synchronized void clusterCompositionLookupFailed( Throwable error )
    {
        CompletableFuture<RoutingTable> routingTableFuture = refreshRoutingTableFuture;
        refreshRoutingTableFuture = null;
        routingTableFuture.completeExceptionally( error );
    }

    public Set<BoltServerAddress> servers()
    {
        return routingTable.servers();
    }
}
