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
package org.neo4j.driver.internal.bolt.api.exception;

import java.io.Serial;
import java.util.Map;
import java.util.Objects;
import org.neo4j.driver.Value;

public class BoltFailureException extends BoltGqlErrorException {
    @Serial
    private static final long serialVersionUID = -1731084000671105197L;

    private final String code;

    public BoltFailureException(
            String code,
            String message,
            String gqlStatus,
            String statusDescription,
            Map<String, Value> diagnosticRecord,
            Throwable cause) {
        super(message, gqlStatus, statusDescription, diagnosticRecord, cause);
        this.code = Objects.requireNonNull(code);
    }

    public String code() {
        return code;
    }
}
