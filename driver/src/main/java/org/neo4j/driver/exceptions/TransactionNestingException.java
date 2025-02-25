/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
import org.neo4j.driver.internal.GqlStatusError;

/**
 * This exception indicates a user is nesting new transaction with an on-going transaction (unmanaged and/or auto-commit).
 */
public class TransactionNestingException extends ClientException {
    @Serial
    private static final long serialVersionUID = -8264319542004457065L;

    /**
     * Creates a new instance.
     * @param message the message
     */
    public TransactionNestingException(String message) {
        super(
                GqlStatusError.UNKNOWN.getStatus(),
                GqlStatusError.UNKNOWN.getStatusDescription(message),
                "N/A",
                message,
                GqlStatusError.DIAGNOSTIC_RECORD,
                null);
    }
}
