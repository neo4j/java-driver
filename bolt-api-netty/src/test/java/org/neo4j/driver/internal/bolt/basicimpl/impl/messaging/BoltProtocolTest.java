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
package org.neo4j.driver.internal.bolt.basicimpl.impl.messaging;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.internal.bolt.basicimpl.impl.async.connection.ChannelAttributes.setProtocolVersion;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.exception.BoltClientException;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v41.BoltProtocolV41;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v42.BoltProtocolV42;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v43.BoltProtocolV43;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v44.BoltProtocolV44;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v5.BoltProtocolV5;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v51.BoltProtocolV51;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v52.BoltProtocolV52;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v53.BoltProtocolV53;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v54.BoltProtocolV54;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v55.BoltProtocolV55;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v56.BoltProtocolV56;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v57.BoltProtocolV57;

class BoltProtocolTest {
    @Test
    void shouldCreateProtocolForKnownVersions() {
        assertAll(
                () -> assertInstanceOf(BoltProtocolV3.class, BoltProtocol.forVersion(BoltProtocolV3.VERSION)),
                () -> assertInstanceOf(BoltProtocolV4.class, BoltProtocol.forVersion(BoltProtocolV4.VERSION)),
                () -> assertInstanceOf(BoltProtocolV41.class, BoltProtocol.forVersion(BoltProtocolV41.VERSION)),
                () -> assertInstanceOf(BoltProtocolV42.class, BoltProtocol.forVersion(BoltProtocolV42.VERSION)),
                () -> assertInstanceOf(BoltProtocolV43.class, BoltProtocol.forVersion(BoltProtocolV43.VERSION)),
                () -> assertInstanceOf(BoltProtocolV44.class, BoltProtocol.forVersion(BoltProtocolV44.VERSION)),
                () -> assertInstanceOf(BoltProtocolV5.class, BoltProtocol.forVersion(BoltProtocolV5.VERSION)),
                () -> assertInstanceOf(BoltProtocolV51.class, BoltProtocol.forVersion(BoltProtocolV51.VERSION)),
                () -> assertInstanceOf(BoltProtocolV52.class, BoltProtocol.forVersion(BoltProtocolV52.VERSION)),
                () -> assertInstanceOf(BoltProtocolV53.class, BoltProtocol.forVersion(BoltProtocolV53.VERSION)),
                () -> assertInstanceOf(BoltProtocolV54.class, BoltProtocol.forVersion(BoltProtocolV54.VERSION)),
                () -> assertInstanceOf(BoltProtocolV55.class, BoltProtocol.forVersion(BoltProtocolV55.VERSION)),
                () -> assertInstanceOf(BoltProtocolV56.class, BoltProtocol.forVersion(BoltProtocolV56.VERSION)),
                () -> assertInstanceOf(BoltProtocolV57.class, BoltProtocol.forVersion(BoltProtocolV57.VERSION)));
    }

    @Test
    void shouldThrowForUnknownVersion() {
        assertAll(
                () -> assertThrows(
                        BoltClientException.class, () -> BoltProtocol.forVersion(new BoltProtocolVersion(42, 0))),
                () -> assertThrows(
                        BoltClientException.class, () -> BoltProtocol.forVersion(new BoltProtocolVersion(142, 0))),
                () -> assertThrows(
                        BoltClientException.class, () -> BoltProtocol.forVersion(new BoltProtocolVersion(-1, 0))));
    }

    @Test
    void shouldThrowForChannelWithUnknownProtocolVersion() {
        var channel = new EmbeddedChannel();
        setProtocolVersion(channel, new BoltProtocolVersion(42, 0));

        assertThrows(BoltClientException.class, () -> BoltProtocol.forChannel(channel));
    }
}
