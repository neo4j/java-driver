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
package org.neo4j.driver.internal.security;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.neo4j.driver.internal.util.Futures.completionExceptionCause;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.AuthTokenManagerExecutionException;

public class ValidatingAuthTokenManager implements AuthTokenManager {
    private final Logger log;
    private final AuthTokenManager delegate;

    public ValidatingAuthTokenManager(AuthTokenManager delegate, Logging logging) {
        requireNonNull(delegate, "delegate must not be null");
        requireNonNull(logging, "logging must not be null");
        this.delegate = delegate;
        this.log = logging.getLog(getClass());
    }

    @Override
    public CompletionStage<AuthToken> getToken() {
        CompletionStage<AuthToken> tokenStage;
        try {
            tokenStage = delegate.getToken();
        } catch (Throwable throwable) {
            tokenStage = failedFuture(throwable);
        }
        if (tokenStage == null) {
            tokenStage = failedFuture(new NullPointerException(String.format(
                    "null returned by %s.getToken method", delegate.getClass().getName())));
        }
        return tokenStage
                .thenApply(token -> Objects.requireNonNull(token, "token must not be null"))
                .handle((token, throwable) -> {
                    if (throwable != null) {
                        throw new AuthTokenManagerExecutionException(
                                String.format(
                                        "invalid execution outcome on %s.getToken method",
                                        delegate.getClass().getName()),
                                completionExceptionCause(throwable));
                    }
                    return token;
                });
    }

    @Override
    public void onExpired(AuthToken authToken) {
        requireNonNull(authToken, "authToken must not be null");
        try {
            delegate.onExpired(authToken);
        } catch (Throwable throwable) {
            log.warn(String.format(
                    "%s has been thrown by %s.onExpired method",
                    throwable.getClass().getName(), delegate.getClass().getName()));
            log.debug(
                    String.format(
                            "%s has been thrown by %s.onExpired method",
                            throwable.getClass().getName(), delegate.getClass().getName()),
                    throwable);
        }
    }
}
