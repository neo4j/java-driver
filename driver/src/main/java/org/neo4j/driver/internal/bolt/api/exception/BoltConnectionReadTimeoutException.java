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

public class BoltConnectionReadTimeoutException extends BoltServiceUnavailableException {
    @Serial
    private static final long serialVersionUID = -5218004917132451861L;

    public BoltConnectionReadTimeoutException(String message) {
        super(message);
    }

    public BoltConnectionReadTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
