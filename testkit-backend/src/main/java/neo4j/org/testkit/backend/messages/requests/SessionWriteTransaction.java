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
package neo4j.org.testkit.backend.messages.requests;

import com.fasterxml.jackson.annotation.JacksonInject;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import neo4j.org.testkit.backend.AsyncSessionState;
import neo4j.org.testkit.backend.CommandProcessor;
import neo4j.org.testkit.backend.SessionState;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.RetryableDone;
import neo4j.org.testkit.backend.messages.responses.RetryableTry;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransactionWork;

@Setter
@Getter
public class SessionWriteTransaction implements TestkitRequest
{
    private final CommandProcessor commandProcessor;

    private SessionWriteTransactionBody data;

    public SessionWriteTransaction( @JacksonInject(CommandProcessor.COMMAND_PROCESSOR_ID) CommandProcessor commandProcessor )
    {
        this.commandProcessor = commandProcessor;
    }

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        return Optional.ofNullable( testkitState.getSessionStates().getOrDefault( data.getSessionId(), null ) )
                       .map( sessionState ->
                             {
                                 Session session = sessionState.getSession();
                                 session.writeTransaction( handle( testkitState, sessionState ) );
                                 return retryableDone();
                             } ).orElseThrow( () -> new RuntimeException( "Could not find session" ) );
    }

    @Override
    public CompletionStage<Optional<TestkitResponse>> processAsync( TestkitState testkitState )
    {
        AsyncSessionState sessionState = testkitState.getAsyncSessionStates().get( data.getSessionId() );
        AsyncSession session = sessionState.getSession();

        AsyncTransactionWork<CompletionStage<Void>> workWrapper =
                tx ->
                {
                    String txId = testkitState.newId();
                    testkitState.getAsyncTransactions().put( txId, tx );
                    testkitState.getResponseWriter().accept( retryableTry( txId ) );
                    CompletableFuture<Void> tryResult = new CompletableFuture<>();
                    sessionState.setTxWorkFuture( tryResult );
                    return tryResult;
                };

        return session.writeTransactionAsync( workWrapper )
                      .thenApply( nothing -> retryableDone() )
                      .thenApply( Optional::of );
    }

    private TransactionWork<Integer> handle( TestkitState testkitState, SessionState sessionState )
    {
        return tx ->
        {
            System.out.println( "Start" );
            sessionState.setRetryableState( 0 );
            String txId = testkitState.newId();
            testkitState.getTransactions().put( txId, tx );
            testkitState.getResponseWriter().accept( retryableTry( txId ) );

            while ( true )
            {
                // Process commands as usual but blocking in here
                commandProcessor.process();

                // Check if state changed on session
                switch ( sessionState.retryableState )
                {
                case 0:
                    // Nothing happened to session state while processing command
                    break;
                case 1:
                    // Client is happy to commit
                    return 0;
                case -1:
                    // Client wants to rollback
                    if ( !"".equals( sessionState.retryableErrorId ) )
                    {
                        throw testkitState.getErrors().get( sessionState.retryableErrorId );
                    }
                    else
                    {
                        throw new RuntimeException( "Error from client in retryable tx" );
                    }
                }
            }
        };
    }

    private RetryableTry retryableTry( String txId )
    {
        return RetryableTry.builder().data( RetryableTry.RetryableTryBody.builder().id( txId ).build() ).build();
    }

    private RetryableDone retryableDone()
    {
        return RetryableDone.builder().build();
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class SessionWriteTransactionBody
    {
        private String sessionId;
        private Map<String,String> txMeta;
        private String timeout;
    }
}
