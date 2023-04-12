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
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.internal.util.LockUtil.executeWithLock;

import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokenAndExpiration;
import org.neo4j.driver.AuthTokenManager;

public class ExpirationBasedAuthTokenManager implements AuthTokenManager {
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Supplier<CompletionStage<AuthTokenAndExpiration>> freshTokenSupplier;
    private final Clock clock;
    private CompletableFuture<AuthToken> tokenFuture;
    private AuthTokenAndExpiration token;

    public ExpirationBasedAuthTokenManager(
            Supplier<CompletionStage<AuthTokenAndExpiration>> freshTokenSupplier, Clock clock) {
        this.freshTokenSupplier = freshTokenSupplier;
        this.clock = clock;
    }

    public CompletionStage<AuthToken> getToken() {
        var validTokenFuture = executeWithLock(lock.readLock(), this::getValidTokenFuture);
        if (validTokenFuture == null) {
            var fetchFromUpstream = new AtomicBoolean();
            validTokenFuture = executeWithLock(lock.writeLock(), () -> {
                if (getValidTokenFuture() == null) {
                    tokenFuture = new CompletableFuture<>();
                    token = null;
                    fetchFromUpstream.set(true);
                }
                return tokenFuture;
            });
            if (fetchFromUpstream.get()) {
                getFromUpstream().whenComplete(this::handleUpstreamResult);
            }
        }
        return validTokenFuture;
    }

    public void onExpired(AuthToken authToken) {
        executeWithLock(lock.writeLock(), () -> {
            if (token != null && token.authToken().equals(authToken)) {
                unsetTokenState();
            }
        });
    }

    private void handleUpstreamResult(AuthTokenAndExpiration authTokenAndExpiration, Throwable throwable) {
        if (throwable != null) {
            var previousTokenFuture = executeWithLock(lock.writeLock(), this::unsetTokenState);
            // notify downstream consumers of the failure
            previousTokenFuture.completeExceptionally(throwable);
        } else {
            if (isValid(authTokenAndExpiration)) {
                var previousTokenFuture = executeWithLock(lock.writeLock(), this::unsetTokenState);
                // notify downstream consumers of the invalid token
                previousTokenFuture.completeExceptionally(
                        new IllegalStateException("invalid token served by upstream"));
            } else {
                var currentTokenFuture = executeWithLock(lock.writeLock(), () -> {
                    token = authTokenAndExpiration;
                    return tokenFuture;
                });
                currentTokenFuture.complete(authTokenAndExpiration.authToken());
            }
        }
    }

    private CompletableFuture<AuthToken> unsetTokenState() {
        var previousTokenFuture = tokenFuture;
        tokenFuture = null;
        token = null;
        return previousTokenFuture;
    }

    private CompletionStage<AuthTokenAndExpiration> getFromUpstream() {
        CompletionStage<AuthTokenAndExpiration> upstreamStage;
        try {
            upstreamStage = freshTokenSupplier.get();
            requireNonNull(upstreamStage, "upstream supplied a null value");
        } catch (Throwable t) {
            upstreamStage = failedFuture(t);
        }
        return upstreamStage;
    }

    private boolean isValid(AuthTokenAndExpiration token) {
        return token == null || token.expirationTimestamp() < clock.millis();
    }

    private CompletableFuture<AuthToken> getValidTokenFuture() {
        CompletableFuture<AuthToken> validTokenFuture = null;
        if (tokenFuture != null) {
            if (token != null) {
                var expirationTimestamp = token.expirationTimestamp();
                validTokenFuture = expirationTimestamp > clock.millis() ? tokenFuture : null;
            } else {
                validTokenFuture = tokenFuture;
            }
        }
        return validTokenFuture;
    }
}
