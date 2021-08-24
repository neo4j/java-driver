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

import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import neo4j.org.testkit.backend.messages.responses.Transaction;

import java.util.concurrent.CompletionStage;

@Getter

@Setter
public class TransactionRollback implements TestkitRequest
{
    private TransactionRollbackBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        testkitState.getTransaction( data.getTxId() ).rollback();
        return createResponse( data.getTxId() );
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync( TestkitState testkitState )
    {
        return testkitState.getAsyncTransaction( data.getTxId() ).thenCompose( tx -> tx.rollbackAsync() ).thenApply( ignored -> createResponse( data.getTxId() ) );
    }

    private Transaction createResponse( String txId )
    {
        return Transaction.builder().data( Transaction.TransactionBody.builder().id( txId ).build() ).build();
    }

    @Getter
    @Setter
    public static class TransactionRollbackBody
    {
        private String txId;
    }
}
