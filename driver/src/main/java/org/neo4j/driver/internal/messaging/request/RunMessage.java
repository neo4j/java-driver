/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.driver.internal.messaging.request;

import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.v1.Value;

import static java.lang.String.format;

/**
 * RUN request message
 * <p>
 * Sent by clients to start a new Tank job for a given statement and
 * parameter set.
 */
public class RunMessage implements Message
{
    public final static byte SIGNATURE = 0x10;

    private final String statement;
    private final Map<String,Value> parameters;

    public RunMessage( String statement )
    {
        this( statement, Collections.emptyMap() );
    }

    public RunMessage( String statement, Map<String,Value> parameters )
    {
        this.statement = statement;
        this.parameters = parameters;
    }

    public String statement()
    {
        return statement;
    }

    public Map<String,Value> parameters()
    {
        return parameters;
    }

    @Override
    public byte signature()
    {
        return SIGNATURE;
    }

    @Override
    public String toString()
    {
        return format( "RUN \"%s\" %s", statement, parameters );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        RunMessage that = (RunMessage) o;

        return !(parameters != null ? !parameters.equals( that.parameters ) : that.parameters != null) &&
               !(statement != null ? !statement.equals( that.statement ) : that.statement != null);

    }

    @Override
    public int hashCode()
    {
        int result = statement != null ? statement.hashCode() : 0;
        result = 31 * result + (parameters != null ? parameters.hashCode() : 0);
        return result;
    }
}
