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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static java.lang.String.format;
import static org.neo4j.driver.internal.async.ProtocolUtil.handshake;

public class ChannelConnectedListener implements ChannelFutureListener
{
    private final BoltServerAddress address;
    private final ChannelPipelineBuilder pipelineBuilder;
    private final ChannelPromise handshakeCompletedPromise;
    private final Logging logging;
    private final Logger log;

    public ChannelConnectedListener( BoltServerAddress address, ChannelPipelineBuilder pipelineBuilder,
            ChannelPromise handshakeCompletedPromise, Logging logging )
    {
        this.address = address;
        this.pipelineBuilder = pipelineBuilder;
        this.handshakeCompletedPromise = handshakeCompletedPromise;
        this.logging = logging;
        this.log = logging.getLog( getClass().getSimpleName() );
    }

    @Override
    public void operationComplete( ChannelFuture future )
    {
        Channel channel = future.channel();

        if ( future.isSuccess() )
        {
            log.trace( "Channel %s connected, running bolt handshake", channel );

            ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast( new HandshakeHandler( pipelineBuilder, handshakeCompletedPromise, logging ) );
            ChannelFuture handshakeFuture = channel.writeAndFlush( handshake() );

            handshakeFuture.addListener( channelFuture ->
            {
                if ( !channelFuture.isSuccess() )
                {
                    handshakeCompletedPromise.setFailure( channelFuture.cause() );
                }
            } );
        }
        else
        {
            handshakeCompletedPromise.setFailure( databaseUnavailableError( address, future.cause() ) );
        }
    }

    private static Throwable databaseUnavailableError( BoltServerAddress address, Throwable cause )
    {
        return new ServiceUnavailableException( format(
                "Unable to connect to %s, ensure the database is running and that there " +
                "is a working network connection to it.", address ), cause );
    }
}
