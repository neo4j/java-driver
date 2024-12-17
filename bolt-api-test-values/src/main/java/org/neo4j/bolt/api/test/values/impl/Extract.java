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
package org.neo4j.bolt.api.test.values.impl;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import org.neo4j.driver.internal.bolt.api.values.Value;

public final class Extract {
    private Extract() {
        throw new UnsupportedOperationException();
    }

    public static <T> Map<String, T> map(Map<String, Value> data, Function<Value, T> mapFunction) {
        if (data.isEmpty()) {
            return emptyMap();
        } else {
            var size = data.size();
            if (size == 1) {
                var head = data.entrySet().iterator().next();
                return singletonMap(head.getKey(), mapFunction.apply(head.getValue()));
            } else {
                Map<String, T> map = new LinkedHashMap<>(size);
                for (var entry : data.entrySet()) {
                    map.put(entry.getKey(), mapFunction.apply(entry.getValue()));
                }
                return unmodifiableMap(map);
            }
        }
    }
}
