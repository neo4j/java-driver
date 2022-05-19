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
package org.neo4j.docs.driver;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.net.ServerAddress;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class RoutingExamplesIT {
    private static final String NEO4J_VERSION =
            Optional.ofNullable(System.getenv("NEO4J_VERSION")).orElse("4.4");

    @Container
    private static final Neo4jContainer<?> NEO4J_CONTAINER = new Neo4jContainer<>(
                    String.format("neo4j:%s-enterprise", NEO4J_VERSION))
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withAdminPassword(null);

    @Test
    void testShouldRunConfigCustomResolverExample() throws Exception {
        // Given
        URI boltUri = URI.create(NEO4J_CONTAINER.getBoltUrl());
        String neo4jUrl = String.format("neo4j://%s:%d", boltUri.getHost(), boltUri.getPort());
        try (ConfigCustomResolverExample example = new ConfigCustomResolverExample(
                neo4jUrl, AuthTokens.none(), ServerAddress.of(boltUri.getHost(), boltUri.getPort()))) {
            // Then
            assertTrue(example.canConnect());
        }
    }
}
