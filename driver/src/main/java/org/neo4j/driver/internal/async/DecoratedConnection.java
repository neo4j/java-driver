/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.driver.internal.async;

import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.AccessMode;

/**
 * This is a connection with extra parameters such as database name and access mode
 */
public class DecoratedConnection implements Connection
{
    private final Connection delegate;
    private final AccessMode mode;
    private final String databaseName;

    public DecoratedConnection( Connection delegate, String databaseName, AccessMode mode )
    {
        this.delegate = delegate;
        this.mode = mode;
        this.databaseName = databaseName;
    }

    public Connection connection()
    {
        return delegate;
    }

    @Override
    public boolean isOpen()
    {
        return delegate.isOpen();
    }

    @Override
    public void enableAutoRead()
    {
        delegate.enableAutoRead();
    }

    @Override
    public void disableAutoRead()
    {
        delegate.disableAutoRead();
    }

    @Override
    public void write( Message message, ResponseHandler handler )
    {
        delegate.write( message, handler );
    }

    @Override
    public void write( Message message1, ResponseHandler handler1, Message message2, ResponseHandler handler2 )
    {
        delegate.write( message1, handler1, message2, handler2 );
    }

    @Override
    public void writeAndFlush( Message message, ResponseHandler handler )
    {
        delegate.writeAndFlush( message, handler );
    }

    @Override
    public void writeAndFlush( Message message1, ResponseHandler handler1, Message message2, ResponseHandler handler2 )
    {
        delegate.writeAndFlush( message1, handler1, message2, handler2 );
    }

    @Override
    public CompletionStage<Void> reset()
    {
        return delegate.reset();
    }

    @Override
    public CompletionStage<Void> release()
    {
        return delegate.release();
    }

    @Override
    public void terminateAndRelease( String reason )
    {
        delegate.terminateAndRelease( reason );
    }

    @Override
    public BoltServerAddress serverAddress()
    {
        return delegate.serverAddress();
    }

    @Override
    public ServerVersion serverVersion()
    {
        return delegate.serverVersion();
    }

    @Override
    public BoltProtocol protocol()
    {
        return delegate.protocol();
    }

    @Override
    public AccessMode mode()
    {
        return mode;
    }

    @Override
    public String databaseName()
    {
        return this.databaseName;
    }

    @Override
    public void flush()
    {
        delegate.flush();
    }
}
