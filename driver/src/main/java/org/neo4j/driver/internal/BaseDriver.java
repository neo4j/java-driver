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

package org.neo4j.driver.internal;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Session;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

abstract class BaseDriver implements Driver
{
    private final SecurityPlan securityPlan;
    protected final Logger log;
    protected final List<BoltServerAddress> servers = new LinkedList<>();

    BaseDriver( BoltServerAddress address, SecurityPlan securityPlan, Logging logging )
    {
        this.servers.add( address );
        this.securityPlan = securityPlan;
        this.log = logging.getLog( Session.LOG_NAME );
    }

    @Override
    public boolean isEncrypted()
    {
        return securityPlan.requiresEncryption();
    }

    @Override
    public List<BoltServerAddress> servers()
    {
        return Collections.unmodifiableList( servers );
    }

    protected BoltServerAddress randomServer()
    {
        return servers.get( ThreadLocalRandom.current().nextInt( 0, servers.size() ) );
    }

}
