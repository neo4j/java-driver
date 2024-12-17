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
import java.util.Optional;
import org.neo4j.driver.internal.bolt.api.values.Value;

public class BoltGqlErrorException extends BoltException {
    @Serial
    private static final long serialVersionUID = -1731084000671105197L;

    private final String gqlStatus;
    private final String statusDescription;
    private final Map<String, Value> diagnosticRecord;

    public BoltGqlErrorException(
            String message,
            String gqlStatus,
            String statusDescription,
            Map<String, Value> diagnosticRecord,
            Throwable cause) {
        super(message, cause);
        this.gqlStatus = Objects.requireNonNull(gqlStatus);
        this.statusDescription = Objects.requireNonNull(statusDescription);
        this.diagnosticRecord = Objects.requireNonNull(diagnosticRecord);
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

    public Optional<BoltGqlErrorException> gqlCause() {
        return findFirstGqlCause(this, BoltGqlErrorException.class);
    }

    @SuppressWarnings("DuplicatedCode")
    private static <T extends Throwable> Optional<T> findFirstGqlCause(Throwable throwable, Class<T> targetCls) {
        var cause = throwable.getCause();
        if (cause == null) {
            return Optional.empty();
        }
        if (cause.getClass().isAssignableFrom(targetCls)) {
            return Optional.of(targetCls.cast(cause));
        } else {
            return findFirstGqlCause(cause, targetCls);
        }
    }
}
