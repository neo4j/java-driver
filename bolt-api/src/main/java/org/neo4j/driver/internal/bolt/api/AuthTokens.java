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
package org.neo4j.driver.internal.bolt.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.neo4j.driver.internal.bolt.api.values.Value;
import org.neo4j.driver.internal.bolt.api.values.ValueFactory;

public final class AuthTokens {
    private static final String SCHEME_KEY = "scheme";
    private static final String PRINCIPAL_KEY = "principal";
    private static final String CREDENTIALS_KEY = "credentials";
    private static final String REALM_KEY = "realm";
    private static final String PARAMETERS_KEY = "parameters";

    private AuthTokens() {}

    public static AuthToken basic(String username, String password, String realm, ValueFactory valueFactory) {
        Objects.requireNonNull(username);
        Objects.requireNonNull(password);
        Objects.requireNonNull(valueFactory);

        var map = new HashMap<String, Value>(4);
        map.put(SCHEME_KEY, valueFactory.value("basic"));
        map.put(PRINCIPAL_KEY, valueFactory.value(username));
        map.put(CREDENTIALS_KEY, valueFactory.value(password));
        if (realm != null) {
            map.put(REALM_KEY, valueFactory.value(realm));
        }
        return new AuthTokenImpl(Collections.unmodifiableMap(map));
    }

    public static AuthToken bearer(String token, ValueFactory valueFactory) {
        Objects.requireNonNull(token);
        Objects.requireNonNull(valueFactory);

        var map = new HashMap<String, Value>(2);
        map.put(SCHEME_KEY, valueFactory.value("bearer"));
        map.put(CREDENTIALS_KEY, valueFactory.value(token));
        return new AuthTokenImpl(Collections.unmodifiableMap(map));
    }

    public static AuthToken kerberos(String base64EncodedTicket, ValueFactory valueFactory) {
        Objects.requireNonNull(base64EncodedTicket);
        Objects.requireNonNull(valueFactory);

        var map = new HashMap<String, Value>(3);
        map.put(SCHEME_KEY, valueFactory.value("kerberos"));
        map.put(PRINCIPAL_KEY, valueFactory.value("")); // This empty string is required for backwards compatibility.
        map.put(CREDENTIALS_KEY, valueFactory.value(base64EncodedTicket));
        return new AuthTokenImpl(Collections.unmodifiableMap(map));
    }

    public static AuthToken none(ValueFactory valueFactory) {
        Objects.requireNonNull(valueFactory);

        return new AuthTokenImpl(Collections.singletonMap(SCHEME_KEY, valueFactory.value("none")));
    }

    public static AuthToken custom(Map<String, Value> map) {
        Objects.requireNonNull(map);

        return new AuthTokenImpl(Collections.unmodifiableMap(map));
    }
}
