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

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Metrics;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionParameters;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.internal.reactive.InternalRxSession;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.reactive.RxSession;

import static org.neo4j.driver.SessionParameters.builder;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;

public class InternalDriver implements Driver
{
    private final SecurityPlan securityPlan;
    private final SessionFactory sessionFactory;
    private final Logger log;

    private AtomicBoolean closed = new AtomicBoolean( false );
    private final MetricsProvider metricsProvider;

    InternalDriver( SecurityPlan securityPlan, SessionFactory sessionFactory, MetricsProvider metricsProvider, Logging logging )
    {
        this.securityPlan = securityPlan;
        this.sessionFactory = sessionFactory;
        this.metricsProvider = metricsProvider;
        this.log = logging.getLog( Driver.class.getSimpleName() );
    }

    @Override
    public Session session()
    {
        return session( SessionParameters.empty() );
    }

    @Override
    public Session session( SessionParameters parameters )
    {
        return newSession( parameters );
    }

    @Override
    public Session session( String databaseName )
    {
        return session( builder().withDatabaseName( databaseName ).build() );
    }

    @Override
    public Session session( AccessMode mode )
    {
        return session( builder().withAccessMode( mode ).build() );
    }

    @Override
    public Session session( String databaseName, AccessMode mode )
    {
        return session( builder().withDatabaseName( databaseName ).withAccessMode( mode ).build() );
    }

    @Override
    public RxSession rxSession()
    {
        return rxSession( SessionParameters.empty() );
    }

    @Override
    public RxSession rxSession( SessionParameters parameters )
    {
        return new InternalRxSession( newSession( parameters ) );
    }

    @Override
    public RxSession rxSession( String databaseName )
    {
        return rxSession( builder().withDatabaseName( databaseName ).build() );
    }

    @Override
    public RxSession rxSession( AccessMode mode )
    {
        return rxSession( builder().withAccessMode( mode ).build() );
    }

    @Override
    public RxSession rxSession( String databaseName, AccessMode mode )
    {
        return rxSession( builder().withDatabaseName( databaseName ).withAccessMode( mode ).build() );
    }

    @Override
    public AsyncSession asyncSession()
    {
        return asyncSession( SessionParameters.empty() );
    }

    @Override
    public AsyncSession asyncSession( SessionParameters parameters )
    {
        return newSession( parameters );
    }

    @Override
    public AsyncSession asyncSession( String databaseName )
    {
        return asyncSession( builder().withDatabaseName( databaseName ).build() );
    }

    @Override
    public AsyncSession asyncSession( AccessMode mode )
    {
        return asyncSession( builder().withAccessMode( mode ).build() );
    }

    @Override
    public AsyncSession asyncSession( String databaseName, AccessMode mode )
    {
        return asyncSession( builder().withDatabaseName( databaseName ).withAccessMode( mode ).build() );
    }

    @Override
    public Metrics metrics()
    {
        return metricsProvider.metrics();
    }

    @Override
    public boolean isEncrypted()
    {
        assertOpen();
        return securityPlan.requiresEncryption();
    }

    @Override
    public void close()
    {
        Futures.blockingGet( closeAsync() );
    }

    @Override
    public CompletionStage<Void> closeAsync()
    {
        if ( closed.compareAndSet( false, true ) )
        {
            log.info( "Closing driver instance %s", hashCode() );
            return sessionFactory.close();
        }
        return completedWithNull();
    }

    public CompletionStage<Void> verifyConnectivity()
    {
        return sessionFactory.verifyConnectivity();
    }

    /**
     * Get the underlying session factory.
     * <p>
     * <b>This method is only for testing</b>
     *
     * @return the session factory used by this driver.
     */
    public SessionFactory getSessionFactory()
    {
        return sessionFactory;
    }

    private static RuntimeException driverCloseException()
    {
        return new IllegalStateException( "This driver instance has already been closed" );
    }

    private NetworkSession newSession( SessionParameters parameters )
    {
        assertOpen();
        NetworkSession session = sessionFactory.newInstance( parameters );
        if ( closed.get() )
        {
            // session does not immediately acquire connection, it is fine to just throw
            throw driverCloseException();
        }
        return session;
    }

    private void assertOpen()
    {
        if ( closed.get() )
        {
            throw driverCloseException();
        }
    }
}
