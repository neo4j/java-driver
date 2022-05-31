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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.holder.ResultCursorHolder;
import neo4j.org.testkit.backend.holder.ResultHolder;
import neo4j.org.testkit.backend.holder.RxResultHolder;
import neo4j.org.testkit.backend.holder.SessionHolder;
import neo4j.org.testkit.backend.messages.requests.deserializer.TestkitCypherParamDeserializer;
import neo4j.org.testkit.backend.messages.responses.Result;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.Query;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class SessionRun implements TestkitRequest {
    private SessionRunBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        SessionHolder sessionHolder = testkitState.getSessionHolder(data.getSessionId());
        Session session = sessionHolder.getSession();
        Query query = Optional.ofNullable(data.params)
                .map(params -> new Query(data.cypher, data.params))
                .orElseGet(() -> new Query(data.cypher));
        TransactionConfig.Builder transactionConfig = TransactionConfig.builder();
        Optional.ofNullable(data.getTxMeta()).ifPresent(transactionConfig::withMetadata);
        Optional.ofNullable(data.getTimeout()).ifPresent(to -> transactionConfig.withTimeout(Duration.ofMillis(to)));
        org.neo4j.driver.Result result = session.run(query, transactionConfig.build());
        String id = testkitState.addResultHolder(new ResultHolder(sessionHolder, result));

        return createResponse(id, result.keys());
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return testkitState.getAsyncSessionHolder(data.getSessionId()).thenCompose(sessionHolder -> {
            AsyncSession session = sessionHolder.getSession();
            Query query = Optional.ofNullable(data.params)
                    .map(params -> new Query(data.cypher, data.params))
                    .orElseGet(() -> new Query(data.cypher));
            TransactionConfig.Builder transactionConfig = TransactionConfig.builder();
            Optional.ofNullable(data.getTxMeta()).ifPresent(transactionConfig::withMetadata);
            Optional.ofNullable(data.getTimeout())
                    .ifPresent(to -> transactionConfig.withTimeout(Duration.ofMillis(to)));

            return session.runAsync(query, transactionConfig.build()).thenApply(resultCursor -> {
                String id = testkitState.addAsyncResultHolder(new ResultCursorHolder(sessionHolder, resultCursor));
                return createResponse(id, resultCursor.keys());
            });
        });
    }

    @Override
    public Mono<TestkitResponse> processRx(TestkitState testkitState) {
        return testkitState.getRxSessionHolder(data.getSessionId()).flatMap(sessionHolder -> {
            RxSession session = sessionHolder.getSession();
            Query query = Optional.ofNullable(data.params)
                    .map(params -> new Query(data.cypher, data.params))
                    .orElseGet(() -> new Query(data.cypher));
            TransactionConfig.Builder transactionConfig = TransactionConfig.builder();
            Optional.ofNullable(data.getTxMeta()).ifPresent(transactionConfig::withMetadata);
            Optional.ofNullable(data.getTimeout())
                    .ifPresent(to -> transactionConfig.withTimeout(Duration.ofMillis(to)));

            RxResult result = session.run(query, transactionConfig.build());
            String id = testkitState.addRxResultHolder(new RxResultHolder(sessionHolder, result));

            // The keys() method causes RUN message exchange.
            // However, it does not currently report errors.
            return Mono.fromDirect(result.keys()).map(keys -> createResponse(id, keys));
        });
    }

    private Result createResponse(String resultId, List<String> keys) {
        return Result.builder()
                .data(Result.ResultBody.builder().id(resultId).keys(keys).build())
                .build();
    }

    @Setter
    @Getter
    public static class SessionRunBody {
        @JsonDeserialize(using = TestkitCypherParamDeserializer.class)
        private Map<String, Object> params;

        private String sessionId;
        private String cypher;
        private Map<String, Object> txMeta;
        private Integer timeout;
    }
}
