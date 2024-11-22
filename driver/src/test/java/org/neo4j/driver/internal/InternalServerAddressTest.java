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
package org.neo4j.driver.internal;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.internal.bolt.api.BoltServerAddress.DEFAULT_PORT;

import java.net.URI;
import java.net.URISyntaxException;
import org.junit.jupiter.api.Test;

class InternalServerAddressTest {
    @Test
    void defaultPortShouldBe7687() {
        assertThat(DEFAULT_PORT, equalTo(7687));
    }

    @Test
    void portShouldUseDefaultIfNotSupplied() throws URISyntaxException {
        assertThat(
                new InternalServerAddress(new URI("neo4j://localhost")).port(),
                equalTo(InternalServerAddress.DEFAULT_PORT));
    }

    @Test
    void shouldHaveCorrectToString() {
        assertEquals("localhost:4242", new InternalServerAddress("localhost", 4242).toString());
        assertEquals("127.0.0.1:8888", new InternalServerAddress("127.0.0.1", 8888).toString());
    }

    @Test
    void shouldVerifyHost() {
        assertThrows(NullPointerException.class, () -> new InternalServerAddress(null, 0));
    }

    @Test
    void shouldVerifyPort() {
        assertThrows(IllegalArgumentException.class, () -> new InternalServerAddress("localhost", -1));
        assertThrows(IllegalArgumentException.class, () -> new InternalServerAddress("localhost", -42));
        assertThrows(IllegalArgumentException.class, () -> new InternalServerAddress("localhost", 65_536));
        assertThrows(IllegalArgumentException.class, () -> new InternalServerAddress("localhost", 99_999));
    }

    @Test
    void shouldUseUriWithHostButWithoutPort() {
        var uri = URI.create("bolt://neo4j.com");
        var address = new InternalServerAddress(uri);

        assertEquals("neo4j.com", address.host());
        assertEquals(DEFAULT_PORT, address.port());
    }

    @Test
    void shouldUseUriWithHostAndPort() {
        var uri = URI.create("bolt://neo4j.com:12345");
        var address = new InternalServerAddress(uri);

        assertEquals("neo4j.com", address.host());
        assertEquals(12345, address.port());
    }

    @Test
    void shouldIncludeHostAndPortInToString() {
        var address = new InternalServerAddress("localhost", 8081);
        assertThat(address.toString(), equalTo("localhost:8081"));
    }
}
