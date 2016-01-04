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

package org.neo4j.driver.v1.tck;

import cucumber.api.CucumberOptions;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

import java.io.IOException;

import org.neo4j.driver.v1.util.TestNeo4jSession;

/**
 * The base class to run all cucumber tests
 */
@RunWith( DriverCucumberAdapter.class )
@CucumberOptions( features = {"target/resources/features/BoltTypeSystem.feature", "target/resources/features/BoltChunkingAndDechunking.feature"} )
public class DriverComplianceIT
{
    @ClassRule
    public static TestNeo4jSession session = new TestNeo4jSession();

    public DriverComplianceIT() throws IOException
    {
    }

    public static TestNeo4jSession session()
    {
        return session;
    }
}
