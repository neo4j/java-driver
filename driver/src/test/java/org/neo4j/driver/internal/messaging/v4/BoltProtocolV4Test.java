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
package org.neo4j.driver.internal.messaging.v4;

import org.mockito.ArgumentCaptor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.internal.Bookmarks;
import org.neo4j.driver.internal.BookmarksHolder;
import org.neo4j.driver.internal.DefaultBookmarksHolder;
import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.handlers.BeginTxResponseHandler;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.request.BeginMessage;
import org.neo4j.driver.internal.messaging.request.PullNMessage;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3Test;
import org.neo4j.driver.internal.reactive.cursor.InternalStatementResultCursor;
import org.neo4j.driver.internal.reactive.cursor.StatementResultCursorFactory;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;
import static org.neo4j.driver.util.TestUtil.await;
import static org.neo4j.driver.util.TestUtil.connectionMock;

class BoltProtocolV4Test extends BoltProtocolV3Test
{
    @Override
    protected BoltProtocol createProtocol()
    {
        return BoltProtocolV4.INSTANCE;
    }

    @Override
    protected Class<? extends MessageFormat> expectedMessageFormatType()
    {
        return MessageFormatV4.class;
    }

    @Override
    protected void testFailedRunInAutoCommitTxWithWaitingForResponse( Bookmarks bookmarks, TransactionConfig config, AccessMode mode ) throws Exception
    {
        // Given
        Connection connection = connectionMock( mode, protocol );
        BookmarksHolder bookmarksHolder = new DefaultBookmarksHolder( bookmarks );

        CompletableFuture<InternalStatementResultCursor> cursorFuture =
                protocol.runInAutoCommitTransaction( connection, STATEMENT, bookmarksHolder, config, true ).asyncResult().toCompletableFuture();

        ResponseHandler runHandler = verifySessionRunInvoked( connection, bookmarks, config, mode, ABSENT_DB_NAME );
        assertFalse( cursorFuture.isDone() );

        // When I response to Run message with a failure
        runHandler.onFailure( new RuntimeException() );

        // Then
        assertEquals( bookmarks, bookmarksHolder.getBookmarks() );
        assertTrue( cursorFuture.isDone() );
        assertNotNull( cursorFuture.get() );
    }

    @Override
    protected void testSuccessfulRunInAutoCommitTxWithWaitingForResponse( Bookmarks bookmarks, TransactionConfig config, AccessMode mode ) throws Exception
    {
        // Given
        Connection connection = connectionMock( mode, protocol );
        BookmarksHolder bookmarksHolder = new DefaultBookmarksHolder( bookmarks );

        CompletableFuture<InternalStatementResultCursor> cursorFuture =
                protocol.runInAutoCommitTransaction( connection, STATEMENT, bookmarksHolder, config, true ).asyncResult().toCompletableFuture();

        ResponseHandler runHandler = verifySessionRunInvoked( connection, bookmarks, config, mode, ABSENT_DB_NAME );
        assertFalse( cursorFuture.isDone() );

        // When I response to the run message
        runHandler.onSuccess( emptyMap() );

        // Then
        assertEquals( bookmarks, bookmarksHolder.getBookmarks() );
        assertTrue( cursorFuture.isDone() );
        assertNotNull( cursorFuture.get() );
    }

    @Override
    protected void testRunInExplicitTransactionAndWaitForRunResponse( boolean success, AccessMode mode ) throws Exception
    {
        // Given
        Connection connection = connectionMock( mode, protocol );

        CompletableFuture<InternalStatementResultCursor> cursorFuture =
                protocol.runInExplicitTransaction( connection, STATEMENT, mock( ExplicitTransaction.class ), true ).asyncResult().toCompletableFuture();

        ResponseHandler runHandler = verifyTxRunInvoked( connection );
        assertFalse( cursorFuture.isDone() );

        if ( success )
        {
            runHandler.onSuccess( emptyMap() );
        }
        else
        {
            // When responded with a failure
            runHandler.onFailure( new RuntimeException() );
        }

        // Then
        assertTrue( cursorFuture.isDone() );
        assertNotNull( cursorFuture.get() );
    }

    @Override
    protected void testRunWithoutWaitingForRunResponse( boolean autoCommitTx, TransactionConfig config, AccessMode mode ) throws Exception
    {
        // Given
        Connection connection = connectionMock( mode, protocol );
        Bookmarks initialBookmarks = Bookmarks.from( "neo4j:bookmark:v1:tx987" );

        CompletionStage<InternalStatementResultCursor> cursorStage;
        if ( autoCommitTx )
        {
            BookmarksHolder bookmarksHolder = new DefaultBookmarksHolder( initialBookmarks );
            cursorStage = protocol.runInAutoCommitTransaction( connection, STATEMENT, bookmarksHolder, config, false ).asyncResult();
        }
        else
        {
            cursorStage = protocol.runInExplicitTransaction( connection, STATEMENT, mock( ExplicitTransaction.class ), false ).asyncResult();
        }

        // When I complete it immediately without waiting for any responses to run message
        CompletableFuture<InternalStatementResultCursor> cursorFuture = cursorStage.toCompletableFuture();
        assertTrue( cursorFuture.isDone() );
        assertNotNull( cursorFuture.get() );

        // Then
        if ( autoCommitTx )
        {
            verifySessionRunInvoked( connection, initialBookmarks, config, mode, ABSENT_DB_NAME );
        }
        else
        {
            verifyTxRunInvoked( connection );
        }
    }

    @Override
    protected void testDatabaseNameSupport( boolean autoCommitTx )
    {
        Connection connection = connectionMock( "foo", protocol );
        if ( autoCommitTx )
        {
            StatementResultCursorFactory factory =
                    protocol.runInAutoCommitTransaction( connection, STATEMENT, BookmarksHolder.NO_OP, TransactionConfig.empty(), false );
            await( factory.asyncResult() );
            verifySessionRunInvoked( connection, Bookmarks.empty(), TransactionConfig.empty(), AccessMode.WRITE, "foo" );
        }
        else
        {
            CompletionStage<Void> txStage = protocol.beginTransaction( connection, Bookmarks.empty(), TransactionConfig.empty() );
            await( txStage );
            verifyBeginInvoked( connection, Bookmarks.empty(), TransactionConfig.empty(), AccessMode.WRITE, "foo" );
        }
    }

    private ResponseHandler verifyTxRunInvoked( Connection connection )
    {
        return verifyRunInvoked( connection, RunWithMetadataMessage.explicitTxRunMessage( STATEMENT ) );
    }

    private ResponseHandler verifySessionRunInvoked( Connection connection, Bookmarks bookmarks, TransactionConfig config, AccessMode mode, String databaseName )
    {
        RunWithMetadataMessage runMessage = RunWithMetadataMessage.autoCommitTxRunMessage( STATEMENT, bookmarks, config, mode, databaseName );
        return verifyRunInvoked( connection, runMessage );
    }

    private ResponseHandler verifyRunInvoked( Connection connection, RunWithMetadataMessage runMessage )
    {
        ArgumentCaptor<ResponseHandler> runHandlerCaptor = ArgumentCaptor.forClass( ResponseHandler.class );
        ArgumentCaptor<ResponseHandler> pullHandlerCaptor = ArgumentCaptor.forClass( ResponseHandler.class );

        verify( connection ).writeAndFlush( eq( runMessage ), runHandlerCaptor.capture(), eq( PullNMessage.PULL_ALL ), pullHandlerCaptor.capture() );

        assertThat( runHandlerCaptor.getValue(), instanceOf( RunResponseHandler.class ) );
        assertThat( pullHandlerCaptor.getValue(), instanceOf( PullAllResponseHandler.class ) );

        return runHandlerCaptor.getValue();
    }

    private void verifyBeginInvoked( Connection connection, Bookmarks bookmarks, TransactionConfig config, AccessMode mode, String databaseName )
    {
        ArgumentCaptor<ResponseHandler> beginHandlerCaptor = ArgumentCaptor.forClass( ResponseHandler.class );
        BeginMessage beginMessage = new BeginMessage( bookmarks, config, mode, databaseName );

        if( bookmarks.isEmpty() )
        {
            verify( connection ).write( eq( beginMessage ), eq( NoOpResponseHandler.INSTANCE ) );
        }
        else
        {
            verify( connection ).write( eq( beginMessage ), beginHandlerCaptor.capture() );
            assertThat( beginHandlerCaptor.getValue(), instanceOf( BeginTxResponseHandler.class ) );
        }
    }
}
