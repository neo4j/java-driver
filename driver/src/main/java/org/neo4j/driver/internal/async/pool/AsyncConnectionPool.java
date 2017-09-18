/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.driver.internal.async.pool;

import io.netty.util.concurrent.Future;

import org.neo4j.driver.internal.async.AsyncConnection;
import org.neo4j.driver.internal.async.InternalFuture;
import org.neo4j.driver.internal.net.BoltServerAddress;

public interface AsyncConnectionPool
{
    InternalFuture<AsyncConnection> acquire( BoltServerAddress address );

    void purge( BoltServerAddress address );

    boolean hasAddress( BoltServerAddress address );

    int activeConnections( BoltServerAddress address );

    Future<?> closeAsync();
}
