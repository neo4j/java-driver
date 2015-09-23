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
package org.neo4j.driver.internal.spi;

import org.neo4j.driver.PlanTreeNode;
import org.neo4j.driver.ProfiledPlanTreeNode;
import org.neo4j.driver.StatementStatistics;
import org.neo4j.driver.StatementType;
import org.neo4j.driver.Value;

public interface StreamCollector
{
    StreamCollector NO_OP = new StreamCollector()
    {
        @Override
        public void fieldNames( String[] names )
        {
        }

        @Override
        public void record( Value[] fields )
        {
        }

        @Override
        public void statementType( StatementType type )
        {
        }

        @Override
        public void statementStatistics( StatementStatistics statistics )
        {
        }

        @Override
        public void plan( PlanTreeNode plan )
        {
        }
    };

    void fieldNames( String[] names );

    void record( Value[] fields );

    void statementType( StatementType type);

    void statementStatistics( StatementStatistics statistics );

    void plan( PlanTreeNode plan );
}

