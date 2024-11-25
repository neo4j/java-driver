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
package org.neo4j.driver.internal.async;

import java.util.Objects;
import java.util.function.Function;
import org.neo4j.driver.internal.adaptedbolt.DriverResponseHandler;

final class ErrorMappingResponseHandler extends DelegatingResponseHandler {
    private final Function<Throwable, Throwable> errorMapper;

    ErrorMappingResponseHandler(DriverResponseHandler delegate, Function<Throwable, Throwable> errorMapper) {
        super(delegate);
        this.errorMapper = Objects.requireNonNull(errorMapper);
    }

    @Override
    public void onError(Throwable throwable) {
        delegate.onError(errorMapper.apply(throwable));
    }
}
