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
package org.neo4j.driver.v1;

import javadoctest.DocSnippet;
import javadoctest.DocTestRunner;
import org.junit.Rule;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.List;

import org.neo4j.driver.util.TestNeo4jSession;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;

@RunWith( DocTestRunner.class )
public class DriverDocIT
{
    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    /** @see org.neo4j.driver.v1.Driver */
    public void exampleUsage( DocSnippet snippet )
    {
        // given
        snippet.addImport( List.class );
        snippet.addImport( LinkedList.class );

        // when
        snippet.run();

        // then it should've created a bunch of data
        assertEquals( 3, session.run( "MATCH (n) RETURN count(n)" ).single().get( 0 ).javaInteger() );
        assertThat( (List<String>)snippet.get( "names" ), equalTo( asList("Bob", "Alice", "Tina")) );
    }
}
