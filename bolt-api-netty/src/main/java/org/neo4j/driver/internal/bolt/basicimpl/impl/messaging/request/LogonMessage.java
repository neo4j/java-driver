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
package org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.request;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.neo4j.driver.internal.bolt.api.values.Value;
import org.neo4j.driver.internal.bolt.api.values.ValueFactory;

public class LogonMessage extends MessageWithMetadata {
    public static final byte SIGNATURE = 0x6A;
    private static final String CREDENTIALS_KEY = "credentials";

    private final ValueFactory valueFactory;

    public LogonMessage(Map<String, Value> authMap, ValueFactory valueFactory) {
        super(authMap);
        this.valueFactory = Objects.requireNonNull(valueFactory);
    }

    @Override
    public byte signature() {
        return SIGNATURE;
    }

    @Override
    public String toString() {
        Map<String, Value> metadataCopy = new HashMap<>(metadata());
        metadataCopy.replace(CREDENTIALS_KEY, valueFactory.value("******"));
        return "LOGON " + metadataCopy;
    }
}
