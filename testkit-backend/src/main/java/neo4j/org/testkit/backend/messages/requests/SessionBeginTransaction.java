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

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.holder.AsyncTransactionHolder;
import neo4j.org.testkit.backend.holder.RxTransactionHolder;
import neo4j.org.testkit.backend.holder.SessionHolder;
import neo4j.org.testkit.backend.holder.TransactionHolder;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import neo4j.org.testkit.backend.messages.responses.Transaction;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.reactive.RxSession;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class SessionBeginTransaction implements TestkitRequest {
    private SessionBeginTransactionBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        SessionHolder sessionHolder = testkitState.getSessionHolder(data.getSessionId());
        Session session = sessionHolder.getSession();
        TransactionConfig.Builder builder = TransactionConfig.builder();
        Optional.ofNullable(data.txMeta).ifPresent(builder::withMetadata);

        if (data.getTimeout() != null) {
            builder.withTimeout(Duration.ofMillis(data.getTimeout()));
        }

        org.neo4j.driver.Transaction transaction = session.beginTransaction(builder.build());
        return transaction(testkitState.addTransactionHolder(new TransactionHolder(sessionHolder, transaction)));
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return testkitState.getAsyncSessionHolder(data.getSessionId()).thenCompose(sessionHolder -> {
            AsyncSession session = sessionHolder.getSession();
            TransactionConfig.Builder builder = TransactionConfig.builder();
            Optional.ofNullable(data.txMeta).ifPresent(builder::withMetadata);

            if (data.getTimeout() != null) {
                builder.withTimeout(Duration.ofMillis(data.getTimeout()));
            }

            return session.beginTransactionAsync(builder.build())
                    .thenApply(tx -> transaction(
                            testkitState.addAsyncTransactionHolder(new AsyncTransactionHolder(sessionHolder, tx))));
        });
    }

    @Override
    public Mono<TestkitResponse> processRx(TestkitState testkitState) {
        return testkitState.getRxSessionHolder(data.getSessionId()).flatMap(sessionHolder -> {
            RxSession session = sessionHolder.getSession();
            TransactionConfig.Builder builder = TransactionConfig.builder();
            Optional.ofNullable(data.txMeta).ifPresent(builder::withMetadata);

            if (data.getTimeout() != null) {
                builder.withTimeout(Duration.ofMillis(data.getTimeout()));
            }

            return Mono.fromDirect(session.beginTransaction(builder.build()))
                    .map(tx -> transaction(
                            testkitState.addRxTransactionHolder(new RxTransactionHolder(sessionHolder, tx))));
        });
    }

    private Transaction transaction(String txId) {
        return Transaction.builder()
                .data(Transaction.TransactionBody.builder().id(txId).build())
                .build();
    }

    @Getter
    @Setter
    public static class SessionBeginTransactionBody {
        private String sessionId;
        private Map<String, Object> txMeta;
        private Integer timeout;
    }
}
