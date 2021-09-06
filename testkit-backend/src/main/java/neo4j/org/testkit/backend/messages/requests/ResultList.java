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
import neo4j.org.testkit.backend.messages.responses.Record;
import neo4j.org.testkit.backend.messages.responses.RecordList;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

@Setter
@Getter
public class ResultList implements TestkitRequest
{
    private ResultListBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        return createResponse( testkitState.getResults().get( data.getResultId() ).list() );
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync( TestkitState testkitState )
    {
        return testkitState.getResultCursors().get( data.getResultId() )
                           .listAsync()
                           .thenApply( this::createResponse );
    }

    @Override
    public Mono<TestkitResponse> processRx( TestkitState testkitState )
    {
        throw new UnsupportedOperationException( "Operation not supported" );
    }

    private RecordList createResponse( List<org.neo4j.driver.Record> records )
    {
        List<Record.RecordBody> mappedRecords = records.stream()
                                                       .map( record -> Record.RecordBody.builder().values( record ).build() )
                                                       .collect( Collectors.toList() );
        return RecordList.builder()
                         .data( RecordList.RecordListBody.builder().records( mappedRecords ).build() )
                         .build();
    }

    @Setter
    @Getter
    public static class ResultListBody
    {
        private String resultId;
    }
}
