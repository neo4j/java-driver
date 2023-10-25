/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
package org.neo4j.driver.internal.value;

import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.types.Type;

public class DateTimeValue extends ObjectValueAdapter<ZonedDateTime> {
    public DateTimeValue(ZonedDateTime zonedDateTime) {
        super(zonedDateTime);
    }

    @Override
    public OffsetDateTime asOffsetDateTime() {
        return asZonedDateTime().toOffsetDateTime();
    }

    @Override
    public ZonedDateTime asZonedDateTime() {
        return asObject();
    }

    @Override
    public Type type() {
        return InternalTypeSystem.TYPE_SYSTEM.DATE_TIME();
    }
}
