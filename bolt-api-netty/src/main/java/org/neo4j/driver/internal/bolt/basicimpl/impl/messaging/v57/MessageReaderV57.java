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
package org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v57;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.driver.internal.bolt.api.GqlError;
import org.neo4j.driver.internal.bolt.api.exception.BoltProtocolException;
import org.neo4j.driver.internal.bolt.api.values.Type;
import org.neo4j.driver.internal.bolt.api.values.Value;
import org.neo4j.driver.internal.bolt.api.values.ValueFactory;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.ResponseMessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v5.MessageReaderV5;
import org.neo4j.driver.internal.bolt.basicimpl.impl.packstream.PackInput;

public class MessageReaderV57 extends MessageReaderV5 {
    public MessageReaderV57(PackInput input, ValueFactory valueFactory) {
        super(input, valueFactory);
    }

    @Override
    protected void unpackFailureMessage(ResponseMessageHandler output) throws IOException {
        var params = unpacker.unpackMap();
        var gqlError = unpackGqlError(params);
        output.handleFailureMessage(gqlError);
    }

    protected GqlError unpackGqlError(Map<String, Value> params) {
        var gqlStatus = params.get("gql_status").asString();
        var statusDescription = params.get("description").asString();
        var code = params.getOrDefault("neo4j_code", valueFactory.value("N/A")).asString();
        var message = params.get("message").asString();
        Map<String, Value> diagnosticRecord;
        var diagnosticRecordValue = params.get("diagnostic_record");
        if (diagnosticRecordValue != null && Type.MAP.equals(diagnosticRecordValue.type())) {
            var containsOperation = diagnosticRecordValue.containsKey("OPERATION");
            var containsOperationCode = diagnosticRecordValue.containsKey("OPERATION_CODE");
            var containsCurrentSchema = diagnosticRecordValue.containsKey("CURRENT_SCHEMA");
            if (containsOperation && containsOperationCode && containsCurrentSchema) {
                diagnosticRecord = diagnosticRecordValue.asMap(valueFactory::value);
            } else {
                diagnosticRecord = new HashMap<>(diagnosticRecordValue.asMap(valueFactory::value));
                if (!containsOperation) {
                    diagnosticRecord.put("OPERATION", valueFactory.value(""));
                }
                if (!containsOperationCode) {
                    diagnosticRecord.put("OPERATION_CODE", valueFactory.value("0"));
                }
                if (!containsCurrentSchema) {
                    diagnosticRecord.put("CURRENT_SCHEMA", valueFactory.value("/"));
                }
                diagnosticRecord = Collections.unmodifiableMap(diagnosticRecord);
            }
        } else {
            diagnosticRecord = Map.ofEntries(
                    Map.entry("OPERATION", valueFactory.value("")),
                    Map.entry("OPERATION_CODE", valueFactory.value("0")),
                    Map.entry("CURRENT_SCHEMA", valueFactory.value("/")));
        }

        GqlError gqlError = null;
        var cause = params.get("cause");
        if (cause != null) {
            if (!Type.MAP.equals(cause.type())) {
                throw new BoltProtocolException("Unexpected type");
            }
            gqlError = unpackGqlError(cause.asMap(valueFactory::value));
        }

        return new GqlError(gqlStatus, statusDescription, code, message, diagnosticRecord, gqlError);
    }
}
