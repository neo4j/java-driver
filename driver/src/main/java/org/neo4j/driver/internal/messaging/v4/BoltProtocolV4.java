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

import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.Statement;
import org.neo4j.driver.internal.BookmarksHolder;
import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.handlers.AbstractPullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.reactive.cursor.InternalStatementResultCursorFactory;
import org.neo4j.driver.internal.reactive.cursor.StatementResultCursorFactory;
import org.neo4j.driver.internal.spi.Connection;

import static org.neo4j.driver.internal.handlers.PullHandlers.newBoltV3PullAllHandler;
import static org.neo4j.driver.internal.handlers.PullHandlers.newBoltV4PullHandler;

public class BoltProtocolV4 extends BoltProtocolV3
{
    public static final int VERSION = 4;
    public static final BoltProtocol INSTANCE = new BoltProtocolV4();

    @Override
    public MessageFormat createMessageFormat()
    {
        return new MessageFormatV4();
    }

    @Override
    protected StatementResultCursorFactory buildResultCursorFactory( Connection connection, Statement statement, BookmarksHolder bookmarksHolder,
            ExplicitTransaction tx, RunWithMetadataMessage runMessage, boolean waitForRunResponse )
    {
        CompletableFuture<Throwable> runCompletedFuture = new CompletableFuture<>();

        RunResponseHandler runHandler = new RunResponseHandler( runCompletedFuture, METADATA_EXTRACTOR );

        AbstractPullAllResponseHandler pullAllHandler = newBoltV3PullAllHandler( statement, runHandler, connection, bookmarksHolder, tx );
        BasicPullResponseHandler pullHandler = newBoltV4PullHandler( statement, runHandler, connection, bookmarksHolder, tx );

        return new InternalStatementResultCursorFactory( connection, runMessage, runHandler, pullHandler, pullAllHandler, waitForRunResponse );
    }

    protected void verifyDatabaseNameBeforeTransaction( String databaseName )
    {
        // Bolt V4 accepts database name
    }

    @Override
    public int version()
    {
        return VERSION;
    }
}
