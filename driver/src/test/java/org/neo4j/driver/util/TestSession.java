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
package org.neo4j.driver.util;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.Map;

import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;

/**
 * A little utility for integration testing, this provides tests with a session they can work with.
 * If you want more direct control, have a look at {@link TestNeo4j} instead.
 */
public class TestSession extends TestNeo4j implements Session
{
    private Session realSession;

    @Override
    public Statement apply( final Statement base, Description description )
    {
        return super.apply( new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                try
                {
                    realSession = Neo4jDriver.session();
                    base.evaluate();
                }
                finally
                {
                    if ( realSession != null )
                    {
                        realSession.close();
                    }
                }
            }
        }, description );
    }

    @Override
    public void close()
    {
        throw new UnsupportedOperationException( "Disallowed on this test session" );
    }

    @Override
    public Transaction newTransaction()
    {
        return realSession.newTransaction();
    }

    @Override
    public Result run( String statementText, Map<String,Value> parameters )
    {
        return realSession.run( statementText, parameters );
    }

    @Override
    public Result run( String statementText )
    {
        return realSession.run( statementText );
    }

    @Override
    public Result run( org.neo4j.driver.Statement statement )
    {
        return run( statement.text(), statement.parameters() );
    }
}
