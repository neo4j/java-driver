/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.driver.internal.value;

import org.junit.Test;

import org.neo4j.driver.internal.InternalCypherDuration;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.v1.exceptions.value.Uncoercible;
import org.neo4j.driver.v1.types.CypherDuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DurationValueTest
{
    @Test
    public void shouldHaveCorrectType()
    {
        CypherDuration duration = newDuration( 1, 2, 3, 4 );
        DurationValue durationValue = new DurationValue( duration );
        assertEquals( InternalTypeSystem.TYPE_SYSTEM.DURATION(), durationValue.type() );
    }

    @Test
    public void shouldSupportAsObject()
    {
        CypherDuration duration = newDuration( 11, 22, 33, 44 );
        DurationValue durationValue = new DurationValue( duration );
        assertEquals( duration, durationValue.asObject() );
    }

    @Test
    public void shouldSupportAsOffsetTime()
    {
        CypherDuration duration = newDuration( 111, 222, 333, 444 );
        DurationValue durationValue = new DurationValue( duration );
        assertEquals( duration, durationValue.asCypherDuration() );
    }

    @Test
    public void shouldNotSupportAsLong()
    {
        CypherDuration duration = newDuration( 1111, 2222, 3333, 4444 );
        DurationValue durationValue = new DurationValue( duration );

        try
        {
            durationValue.asLong();
            fail( "Exception expected" );
        }
        catch ( Uncoercible ignore )
        {
        }
    }

    private static CypherDuration newDuration( long months, long days, long seconds, long nanoseconds )
    {
        return new InternalCypherDuration( months, days, seconds, nanoseconds );
    }
}
