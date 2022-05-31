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

import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.holder.SessionHolder;
import neo4j.org.testkit.backend.messages.responses.Bookmarks;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.Bookmark;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class SessionLastBookmarks implements TestkitRequest {
    private SessionLastBookmarksBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        SessionHolder sessionHolder = testkitState.getSessionHolder(data.getSessionId());
        Bookmark bookmark = sessionHolder.getSession().lastBookmark();
        return createResponse(bookmark);
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return testkitState
                .getAsyncSessionHolder(data.getSessionId())
                .thenApply(sessionHolder -> sessionHolder.getSession().lastBookmark())
                .thenApply(this::createResponse);
    }

    @Override
    public Mono<TestkitResponse> processRx(TestkitState testkitState) {
        return testkitState
                .getRxSessionHolder(data.getSessionId())
                .map(sessionHolder -> sessionHolder.getSession().lastBookmark())
                .map(this::createResponse);
    }

    private Bookmarks createResponse(Bookmark bookmark) {
        return Bookmarks.builder()
                .data(Bookmarks.BookmarksBody.builder().bookmarks(bookmark).build())
                .build();
    }

    @Setter
    @Getter
    public static class SessionLastBookmarksBody {
        private String sessionId;
    }
}
