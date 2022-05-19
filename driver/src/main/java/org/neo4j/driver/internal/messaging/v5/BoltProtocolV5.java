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
package org.neo4j.driver.internal.messaging.v5;

import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.BoltProtocolVersion;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.v44.BoltProtocolV44;

public class BoltProtocolV5 extends BoltProtocolV44 {
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion(5, 0);
    public static final BoltProtocol INSTANCE = new BoltProtocolV5();

    @Override
    public MessageFormat createMessageFormat() {
        return new MessageFormatV5();
    }

    @Override
    public BoltProtocolVersion version() {
        return VERSION;
    }
}
