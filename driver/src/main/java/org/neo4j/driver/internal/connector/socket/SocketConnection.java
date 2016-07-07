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
package org.neo4j.driver.internal.connector.socket;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.neo4j.driver.internal.messaging.InitMessage;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.PullAllMessage;
import org.neo4j.driver.internal.messaging.RunMessage;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.internal.spi.StreamCollector;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.Neo4jException;

import static org.neo4j.driver.internal.messaging.AckFailureMessage.ACK_FAILURE;
import static org.neo4j.driver.internal.messaging.DiscardAllMessage.DISCARD_ALL;
import static org.neo4j.driver.internal.messaging.ResetMessage.RESET;

public class SocketConnection implements Connection
{
    private final Queue<Message> pendingMessages = new LinkedList<>();
    private final SocketResponseHandler responseHandler;

    private final SocketClient socket;

    public SocketConnection( String host, int port, Config config )
    {
        Logger logger = config.logging().getLog( String.valueOf( System.currentTimeMillis() ) );

        if( logger.isDebugEnabled() )
        {
            this.responseHandler = new LoggingResponseHandler( logger );
        }
        else
        {
            this.responseHandler = new SocketResponseHandler();
        }

        this.socket = new SocketClient( host, port, config, logger );
        socket.start();
    }

    @Override
    public void init( String clientName, Map<String,Value> authToken )
    {
        queueMessage( new InitMessage( clientName, authToken ), StreamCollector.NO_OP );
        sync();
    }

    @Override
    public void run( String statement, Map<String,Value> parameters, StreamCollector collector )
    {
        queueMessage( new RunMessage( statement, parameters ), collector );
    }

    @Override
    public void discardAll()
    {
        queueMessage( DISCARD_ALL, StreamCollector.NO_OP );
    }

    @Override
    public void pullAll( StreamCollector collector )
    {
        queueMessage( PullAllMessage.PULL_ALL, collector );
    }

    @Override
    public void reset( StreamCollector collector )
    {
        queueMessage( RESET, collector );
    }

    @Override
    public void ackFailure( StreamCollector collector )
    {
        queueMessage( ACK_FAILURE, collector );
    }

    @Override
    public void sync()
    {
        flush();
        receiveAll();
    }

    @Override
    public void flush()
    {
        try
        {
            socket.send( pendingMessages );
        }
        catch ( IOException e )
        {
            String message = e.getMessage();
            throw new ClientException( "Unable to send messages to server: " + message, e );
        }
    }

    private void receiveAll()
    {
        try
        {
            socket.receiveAll( responseHandler );
            assertNoServerFailure();
        }
        catch ( IOException e )
        {
            throw mapRecieveError( e );
        }
    }

    @Override
    public void receiveOne()
    {
        try
        {
            socket.receiveOne( responseHandler );
            assertNoServerFailure();
        }
        catch ( IOException e )
        {
            throw mapRecieveError( e );
        }
    }

    private void assertNoServerFailure()
    {
        if ( responseHandler.serverFailureOccurred() )
        {
            ackFailure( StreamCollector.NO_OP );
            Neo4jException exception = responseHandler.serverFailure();
            responseHandler.clearError();
            throw exception;
        }
    }

    private ClientException mapRecieveError( IOException e )
    {
        String message = e.getMessage();
        if ( message == null )
        {
            return new ClientException( "Unable to read response from server: " + e.getClass().getSimpleName(), e );
        }
        else if ( e instanceof SocketTimeoutException )
        {
            return new ClientException( "Server did not reply within the network timeout limit.", e );
        }
        else
        {
            return new ClientException( "Unable to read response from server: " + message, e );
        }
    }

    private void queueMessage( Message msg, StreamCollector collector )
    {
        pendingMessages.add( msg );
        responseHandler.appendResultCollector( collector );
    }

    @Override
    public void close()
    {
        socket.stop();
    }

    @Override
    public boolean isOpen()
    {
        return socket.isOpen();
    }

    @Override
    public void onError( Runnable runnable )
    {
        throw new UnsupportedOperationException( "Error subscribers are not supported on SocketConnection." );
    }
}
