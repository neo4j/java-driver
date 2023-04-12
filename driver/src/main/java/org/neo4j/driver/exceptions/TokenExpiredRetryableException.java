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
package org.neo4j.driver.exceptions;

import java.io.Serial;
import org.neo4j.driver.AuthTokenManager;

/**
 * The token provided by the {@link AuthTokenManager} has expired.
 * <p>
 * This is a retryable variant of {@link TokenExpiredException} used when the driver has an explicit
 * {@link AuthTokenManager} that might supply a new token following this failure.
 * <p>
 * Error code: Neo.ClientError.Security.TokenExpired
 * @since 5.8
 * @see TokenExpiredException
 * @see AuthTokenManager
 * @see org.neo4j.driver.GraphDatabase#driver(String, AuthTokenManager)
 */
public class TokenExpiredRetryableException extends TokenExpiredException implements RetryableException {
    @Serial
    private static final long serialVersionUID = -6672756500436910942L;

    /**
     * Constructs a new instance.
     * @param code the code
     * @param message the message
     */
    public TokenExpiredRetryableException(String code, String message) {
        super(code, message);
    }
}
