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
package org.neo4j.driver.internal.homedb;

import java.util.Collections;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.internal.security.InternalAuthToken;

public interface HomeDatabaseCacheKey {
    static HomeDatabaseCacheKey of(AuthToken overrideAuthToken, String impersonatedUser) {
        HomeDatabaseCacheKey key;
        if (impersonatedUser != null) {
            key = new MapHomeDatabaseCacheKey(
                    Collections.singletonMap(InternalAuthToken.PRINCIPAL_KEY, impersonatedUser));
        } else if (overrideAuthToken != null) {
            var tokenMap = ((InternalAuthToken) overrideAuthToken).toMap();
            var scheme = tokenMap.get(InternalAuthToken.SCHEME_KEY);
            if (scheme.asString().equals("basic")) {
                key = new MapHomeDatabaseCacheKey(Collections.singletonMap(
                        InternalAuthToken.PRINCIPAL_KEY, tokenMap.get(InternalAuthToken.PRINCIPAL_KEY)));
            } else {
                key = new MapHomeDatabaseCacheKey(tokenMap);
            }
        } else {
            key = DriverHomeDatabaseCacheKey.INSTANCE;
        }
        return key;
    }
}
