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
package org.neo4j.driver.internal.spi;

import java.util.List;

import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.summary.Notification;
import org.neo4j.driver.v1.summary.Plan;
import org.neo4j.driver.v1.summary.ProfiledPlan;
import org.neo4j.driver.v1.summary.StatementType;
import org.neo4j.driver.v1.summary.SummaryCounters;

public interface StreamCollector
{
    StreamCollector NO_OP = new NoOperationStreamCollector();

    StreamCollector ACK_FAILURE = new NoOperationStreamCollector()
    {
        @Override
        public void doneFailure( Neo4jException error )
        {
            throw new ClientException(
                    "Invalid server response message `FAILURE` received for client message `ACK_FAILURE`.", error );
        }

        @Override
        public void doneIgnored()
        {
            throw new ClientException(
                    "Invalid server response message `IGNORED` received for client message `ACK_FAILURE`." );
        }
    };

    class InitStreamCollector extends NoOperationStreamCollector
    {
        private String server;
        @Override
        public void doneIgnored()
        {
            throw new ClientException(
                    "Invalid server response message `IGNORED` received for client message `INIT`." );
        }

        @Override
        public void server( String server )
        {
            this.server = server;
        }

        public String server()
        {
            return server;
        }
    }

    StreamCollector RESET = new NoOperationStreamCollector()
    {
        @Override
        public void doneFailure( Neo4jException error )
        {
            throw new ClientException(
                    "Invalid server response message `FAILURE` received for client message `RESET`.", error );
        }

        @Override
        public void doneIgnored()
        {
            throw new ClientException(
                    "Invalid server response message `IGNORED` received for client message `RESET`." );
        }
    };

    class NoOperationStreamCollector implements StreamCollector
    {
        @Override
        public void keys( String[] names ) {}

        @Override
        public void record( Value[] fields ) {}

        @Override
        public void statementType( StatementType type ) {}

        @Override
        public void statementStatistics( SummaryCounters statistics ) {}

        @Override
        public void plan( Plan plan ) {}

        @Override
        public void profile( ProfiledPlan plan ) {}

        @Override
        public void notifications( List<Notification> notifications ) {}

        @Override
        public void done() {}

        @Override
        public void doneSuccess()
        {
            done();
        }

        @Override
        public void doneFailure( Neo4jException error )
        {
            done();
        }

        @Override
        public void doneIgnored()
        {
            done();
        }

        @Override
        public void resultAvailableAfter( long l ) {}

        @Override
        public void resultConsumedAfter( long l ) {}

        @Override
        public void server( String server ){}
    }

    // TODO: This should be modified to simply have head/record/tail methods

    void keys( String[] names );

    void record( Value[] fields );

    void statementType( StatementType type);

    void statementStatistics( SummaryCounters statistics );

    void plan( Plan plan );

    void profile( ProfiledPlan plan );

    void notifications( List<Notification> notifications );

    void done();

    void doneSuccess();

    void doneFailure( Neo4jException error );

    void doneIgnored();

    void resultAvailableAfter( long l );

    void resultConsumedAfter( long l );

    void server( String server );
}

