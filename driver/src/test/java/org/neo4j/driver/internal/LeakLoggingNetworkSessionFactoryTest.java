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
package org.neo4j.driver.internal;

import org.junit.Test;

import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Session;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class LeakLoggingNetworkSessionFactoryTest
{
    @Test
    public void createsLeakLoggingNetworkSessions()
    {
        SessionFactory factory = new LeakLoggingNetworkSessionFactory( mock( Logging.class ) );

        Session session = factory.newInstance( mock( PooledConnection.class ) );

        assertThat( session, instanceOf( LeakLoggingNetworkSession.class ) );
    }
}
