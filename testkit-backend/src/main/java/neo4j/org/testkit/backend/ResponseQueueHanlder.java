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
package neo4j.org.testkit.backend;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.Consumer;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

public class ResponseQueueHanlder {
    private final Consumer<TestkitResponse> responseWriter;
    private final Queue<TestkitResponse> responseQueue = new ArrayDeque<>();
    private boolean responseReady;

    ResponseQueueHanlder(Consumer<TestkitResponse> responseWriter) {
        this.responseWriter = responseWriter;
    }

    public synchronized void setResponseReadyAndDispatchFirst() {
        responseReady = true;
        dispatchFirst();
    }

    public synchronized void offerAndDispatchFirst(TestkitResponse response) {
        responseQueue.offer(response);
        if (responseReady) {
            dispatchFirst();
        }
    }

    private synchronized void dispatchFirst() {
        var response = responseQueue.poll();
        if (response != null) {
            responseReady = false;
            responseWriter.accept(response);
        }
    }
}
