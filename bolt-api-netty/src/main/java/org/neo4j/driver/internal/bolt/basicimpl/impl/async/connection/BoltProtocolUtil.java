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
package org.neo4j.driver.internal.bolt.basicimpl.impl.async.connection;

import static io.netty.buffer.Unpooled.copyInt;
import static io.netty.buffer.Unpooled.unreleasableBuffer;
import static java.lang.Integer.toHexString;

import io.netty.buffer.ByteBuf;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.BoltProtocol;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v3.BoltProtocolV3;
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
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v58.BoltProtocolV58;

public final class BoltProtocolUtil {
    public static final int BOLT_MAGIC_PREAMBLE = 0x6060B017;
    public static final BoltProtocolVersion NO_PROTOCOL_VERSION = new BoltProtocolVersion(0, 0);

    public static final int CHUNK_HEADER_SIZE_BYTES = 2;

    public static final int DEFAULT_MAX_OUTBOUND_CHUNK_SIZE_BYTES = Short.MAX_VALUE / 2;

    public static final SortedMap<BoltProtocolVersion, BoltProtocol> versionToProtocol;

    private static final ByteBuf HANDSHAKE_BUF = unreleasableBuffer(copyInt(
                    BOLT_MAGIC_PREAMBLE,
                    0x000001FF,
                    BoltProtocolV58.VERSION.toIntRange(BoltProtocolV5.VERSION),
                    BoltProtocolV44.VERSION.toIntRange(BoltProtocolV42.VERSION),
                    BoltProtocolV3.VERSION.toInt()))
            .asReadOnly();

    private static final String HANDSHAKE_STRING = createHandshakeString();

    static {
        var map = new TreeMap<BoltProtocolVersion, BoltProtocol>(Comparator.reverseOrder());
        map.putAll(Map.ofEntries(
                Map.entry(BoltProtocolV58.VERSION, BoltProtocolV58.INSTANCE),
                Map.entry(BoltProtocolV57.VERSION, BoltProtocolV57.INSTANCE),
                Map.entry(BoltProtocolV56.VERSION, BoltProtocolV56.INSTANCE),
                Map.entry(BoltProtocolV55.VERSION, BoltProtocolV55.INSTANCE),
                Map.entry(BoltProtocolV54.VERSION, BoltProtocolV54.INSTANCE),
                Map.entry(BoltProtocolV53.VERSION, BoltProtocolV53.INSTANCE),
                Map.entry(BoltProtocolV52.VERSION, BoltProtocolV52.INSTANCE),
                Map.entry(BoltProtocolV51.VERSION, BoltProtocolV51.INSTANCE),
                Map.entry(BoltProtocolV5.VERSION, BoltProtocolV5.INSTANCE),
                Map.entry(BoltProtocolV44.VERSION, BoltProtocolV44.INSTANCE),
                Map.entry(BoltProtocolV43.VERSION, BoltProtocolV43.INSTANCE),
                Map.entry(BoltProtocolV42.VERSION, BoltProtocolV42.INSTANCE),
                Map.entry(BoltProtocolV3.VERSION, BoltProtocolV3.INSTANCE)));
        versionToProtocol = Collections.unmodifiableSortedMap(map);
    }

    private BoltProtocolUtil() {}

    public static ByteBuf handshakeBuf() {
        return HANDSHAKE_BUF.duplicate();
    }

    public static String handshakeString() {
        return HANDSHAKE_STRING;
    }

    public static void writeMessageBoundary(ByteBuf buf) {
        buf.writeShort(0);
    }

    public static void writeEmptyChunkHeader(ByteBuf buf) {
        buf.writeShort(0);
    }

    public static void writeChunkHeader(ByteBuf buf, int chunkStartIndex, int headerValue) {
        buf.setShort(chunkStartIndex, headerValue);
    }

    private static String createHandshakeString() {
        var buf = handshakeBuf();
        return String.format(
                "[0x%s, %s, %s, %s, %s]",
                toHexString(buf.readInt()), buf.readInt(), buf.readInt(), buf.readInt(), buf.readInt());
    }
}
