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

import static io.netty.buffer.Unpooled.unreleasableBuffer;
import static java.lang.Integer.toHexString;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.exception.BoltClientException;
import org.neo4j.driver.internal.bolt.basicimpl.impl.logging.ChannelActivityLogger;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.BoltProtocol;

final class ManifestHandlerV1 implements ManifestHandler {
    private final ChannelActivityLogger log;
    private final Channel channel;
    private final VarLongBuilder expectedVersionRangesBuilder = new VarLongBuilder();

    private long expectedVersionRanges = -1L;
    private Set<BoltProtocolMinorVersionRange> serverSupportedVersionRanges;

    public ManifestHandlerV1(Channel channel, LoggingProvider logging) {
        this.channel = Objects.requireNonNull(channel);
        log = new ChannelActivityLogger(channel, logging, getClass());
    }

    @Override
    public void decode(ByteBuf byteBuf) {
        if (expectedVersionRanges < 0) {
            decodeExpectedVersionsSegment(byteBuf);
        } else if (expectedVersionRanges > 0) {
            decodeServerSupportedBoltVersionRange(byteBuf);
        } else {
            byteBuf.readByte();
        }
    }

    @Override
    public BoltProtocol complete() {
        return findSupportedBoltProtocol();
    }

    private void decodeExpectedVersionsSegment(ByteBuf byteBuf) {
        var segment = byteBuf.readByte();
        var value = (byte) (0b01111111 & segment);

        try {
            expectedVersionRangesBuilder.add(value);
        } catch (IllegalStateException e) {
            throw new BoltClientException(
                    "The driver does not support the number of Bolt Protocol version ranges that the server wants to send",
                    e);
        }

        var finished = (segment >> 7) == 0;
        if (finished) {
            expectedVersionRanges = expectedVersionRangesBuilder.build();
            var size = (int) expectedVersionRanges;
            if (expectedVersionRanges != size) {
                throw new BoltClientException(
                        "The driver does not support the number of Bolt Protocol version ranges that the server wants to send");
            } else {
                log.log(
                        System.Logger.Level.DEBUG,
                        "S: [Bolt Handshake Manifest] [expected version ranges %d]",
                        expectedVersionRanges);
                serverSupportedVersionRanges = new HashSet<>(size);
            }
        }
    }

    private void decodeServerSupportedBoltVersionRange(ByteBuf byteBuf) {
        var value = byteBuf.readInt();
        var major = value & 0x000000FF;
        var minor = (value >> 8) & 0x000000FF;
        var minorNum = (value >> 16) & 0x000000FF;
        serverSupportedVersionRanges.add(new BoltProtocolMinorVersionRange(major, minor, minorNum));
        expectedVersionRanges--;

        if (expectedVersionRanges == 0) {
            log.log(
                    System.Logger.Level.DEBUG,
                    "S: [Bolt Handshake Manifest] [server supported version ranges %s]",
                    serverSupportedVersionRanges);
        }
    }

    private BoltProtocol findSupportedBoltProtocol() {
        for (var entry : BoltProtocolUtil.versionToProtocol.entrySet()) {
            var version = entry.getKey();
            for (var range : serverSupportedVersionRanges) {
                if (range.contains(version)) {
                    var protocol = entry.getValue();
                    write(protocol.version().toInt());
                    write((byte) 0);
                    return protocol;
                }
            }
        }
        write(0);
        write((byte) 0);
        channel.flush();
        throw new BoltClientException("No supported Bolt Protocol version was found");
    }

    private void write(int value) {
        log.log(
                System.Logger.Level.DEBUG,
                "C: [Bolt Handshake Manifest] %s",
                String.format("[%s]", toHexString(value)));
        channel.write(Unpooled.copyInt(value).asReadOnly());
    }

    @SuppressWarnings("SameParameterValue")
    private void write(byte value) {
        log.log(
                System.Logger.Level.DEBUG,
                "C: [Bolt Handshake Manifest] %s",
                String.format("[%s]", toHexString(value)));
        channel.write(
                unreleasableBuffer(Unpooled.copiedBuffer(new byte[] {value})).asReadOnly());
    }
}
