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
package org.neo4j.driver.internal.async;

import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.concurrent.Promise;

import java.util.Map;

import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.Value;

import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.async.ChannelAttributes.setLastUsedTimestamp;

public class ReleaseChannelHandler implements ResponseHandler
{
    private final Channel channel;
    private final ChannelPool pool;
    private final Clock clock;
    private final Promise<Void> releasePromise;

    public ReleaseChannelHandler( Channel channel, ChannelPool pool, Clock clock )
    {
        this( channel, pool, clock, null );
    }

    public ReleaseChannelHandler( Channel channel, ChannelPool pool, Clock clock, Promise<Void> releasePromise )
    {
        this.channel = requireNonNull( channel );
        this.pool = requireNonNull( pool );
        this.clock = requireNonNull( clock );
        this.releasePromise = releasePromise;
    }

    @Override
    public void onSuccess( Map<String,Value> metadata )
    {
        releaseChannel();
    }

    @Override
    public void onFailure( Throwable error )
    {
        releaseChannel();
    }

    @Override
    public void onRecord( Value[] fields )
    {
        throw new UnsupportedOperationException();
    }

    private void releaseChannel()
    {
        setLastUsedTimestamp( channel, clock.millis() );

        if ( releasePromise == null )
        {
            pool.release( channel );
        }
        else
        {
            pool.release( channel, releasePromise );
        }
    }
}
