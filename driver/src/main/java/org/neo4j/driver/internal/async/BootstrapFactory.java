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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public final class BootstrapFactory
{
    private BootstrapFactory()
    {
    }

    public static Bootstrap newBootstrap()
    {
        return newBootstrap( new NioEventLoopGroup() );
    }

    public static Bootstrap newBootstrap( int threadCount )
    {
        return newBootstrap( new NioEventLoopGroup( threadCount ) );
    }

    private static Bootstrap newBootstrap( NioEventLoopGroup eventLoopGroup )
    {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group( eventLoopGroup );
        bootstrap.channel( NioSocketChannel.class );
        bootstrap.option( ChannelOption.SO_KEEPALIVE, true );
        bootstrap.option( ChannelOption.SO_REUSEADDR, true );
        return bootstrap;
    }
}
