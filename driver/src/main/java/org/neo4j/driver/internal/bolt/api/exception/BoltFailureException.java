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

public class BoltFailureException extends BoltException {
    @Serial
    private static final long serialVersionUID = 6572184237917326095L;

    private final String code;
    private final String gqlStatus;
    private final String statusDescription;
    private final Map<String, Value> diagnosticRecord;

    public BoltFailureException(
            String gqlStatus,
            String statusDescription,
            String code,
            String message,
            Map<String, Value> diagnosticRecord,
            Throwable cause) {
        super(message, cause);
        this.gqlStatus = Objects.requireNonNull(gqlStatus);
        this.statusDescription = Objects.requireNonNull(statusDescription);
        this.code = code;
        this.diagnosticRecord = Objects.requireNonNull(diagnosticRecord);
    }

    public String code() {
        return code;
    }

    public String gqlStatus() {
        return gqlStatus;
    }

    public String statusDescription() {
        return statusDescription;
    }

    public Map<String, Value> diagnosticRecord() {
        return diagnosticRecord;
    }
}
