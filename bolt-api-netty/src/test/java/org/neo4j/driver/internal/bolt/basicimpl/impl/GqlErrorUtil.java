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
package org.neo4j.driver.internal.bolt.basicimpl.impl;

import java.util.Map;
import org.neo4j.bolt.api.test.values.TestValueFactory;
import org.neo4j.driver.internal.bolt.api.GqlError;
import org.neo4j.driver.internal.bolt.api.GqlStatusError;
import org.neo4j.driver.internal.bolt.api.values.Value;
import org.neo4j.driver.internal.bolt.api.values.ValueFactory;

public class GqlErrorUtil {
    private static final ValueFactory valueFactory = TestValueFactory.INSTANCE;

    public static final Map<String, Value> DIAGNOSTIC_RECORD = Map.ofEntries(
            Map.entry("OPERATION", valueFactory.value("")),
            Map.entry("OPERATION_CODE", valueFactory.value("0")),
            Map.entry("CURRENT_SCHEMA", valueFactory.value("/")));

    public static GqlError gqlError(String code, String message) {
        return new GqlError(
                GqlStatusError.UNKNOWN.getStatus(),
                GqlStatusError.UNKNOWN.getStatusDescription(message),
                code,
                message,
                DIAGNOSTIC_RECORD,
                null);
    }
}
