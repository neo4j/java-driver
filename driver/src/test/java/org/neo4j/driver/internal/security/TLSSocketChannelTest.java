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
package org.neo4j.driver.internal.security;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;

import org.neo4j.driver.internal.logging.DevNullLogger;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.security.TLSSocketChannel.create;

public class TLSSocketChannelTest
{

    @Test
    public void shouldCloseConnectionIfFailedToRead() throws Throwable
    {
        // Given
        ByteChannel mockedChannel = mock( ByteChannel.class );
        SSLEngine mockedSslEngine = mock( SSLEngine.class );
        SSLSession mockedSslSession = mock( SSLSession.class );

        when( mockedChannel.read( any( ByteBuffer.class ) ) ).thenReturn( -1 );
        when ( mockedSslEngine.getSession() ).thenReturn( mockedSslSession );
        when( mockedSslSession.getApplicationBufferSize() ).thenReturn( 10 );
        when( mockedSslSession.getPacketBufferSize() ).thenReturn( 10 );

        // When
        TLSSocketChannel channel = new TLSSocketChannel( mockedChannel, new DevNullLogger(), mockedSslEngine );

        try
        {
            channel.channelRead( ByteBuffer.allocate( 1 ) );
            fail( "Should fail to read" );
        }
        catch( Exception e )
        {
            assertThat( e, instanceOf( ServiceUnavailableException.class ) );
            assertThat( e.getMessage(), startsWith( "SSL Connection terminated while receiving data. " ) );
        }
        // Then
        verify( mockedChannel, times( 1 ) ).close();
    }

    @Test
    public void shouldCloseConnectionIfFailedToWrite() throws Throwable
    {
        // Given
        ByteChannel mockedChannel = mock( ByteChannel.class );
        SSLEngine mockedSslEngine = mock( SSLEngine.class );
        SSLSession mockedSslSession = mock( SSLSession.class );

        when( mockedChannel.write( any( ByteBuffer.class ) ) ).thenReturn( -1 );
        when ( mockedSslEngine.getSession() ).thenReturn( mockedSslSession );
        when( mockedSslSession.getApplicationBufferSize() ).thenReturn( 10 );
        when( mockedSslSession.getPacketBufferSize() ).thenReturn( 10 );

        // When
        TLSSocketChannel channel = new TLSSocketChannel( mockedChannel, new DevNullLogger(), mockedSslEngine );

        try
        {
            channel.channelWrite( ByteBuffer.allocate( 1 ) );
            fail( "Should fail to write" );
        }
        catch( Exception e )
        {
            assertThat( e, instanceOf( ServiceUnavailableException.class ) );
            assertThat( e.getMessage(), startsWith( "SSL Connection terminated while writing data. " ) );
        }

        // Then
        verify( mockedChannel, times( 1 ) ).close();
    }

    @Test
    public void shouldThrowClientErrorIfFailedToHandshake() throws Throwable
    {
        // Given
        ByteChannel mockedChannel = mock( ByteChannel.class );
        SSLEngine mockedSslEngine = mock( SSLEngine.class );
        SSLSession mockedSslSession = mock( SSLSession.class );

        when( mockedChannel.read( any( ByteBuffer.class ) ) ).thenReturn( -1 );
        when ( mockedSslEngine.getSession() ).thenReturn( mockedSslSession );
        when( mockedSslSession.getApplicationBufferSize() ).thenReturn( 10 );
        when( mockedSslSession.getPacketBufferSize() ).thenReturn( 10 );
        doThrow( new SSLHandshakeException( "Failed handshake!" ) ).when( mockedSslEngine ).beginHandshake();

        // When & Then
        try
        {
            create( mockedChannel, new DevNullLogger(), mockedSslEngine );
            fail( "Should fail to run handshake" );
        }
        catch( Exception e )
        {
            assertThat( e, instanceOf( ClientException.class ) );
            assertThat( e.getMessage(), startsWith( "Failed to establish secured connection with the server: Failed handshake!" ) );
        }
        verify( mockedChannel, times( 0 ) ).close();
    }
}
