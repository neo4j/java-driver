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
package org.neo4j.driver.internal.bolt.routedimpl.impl.cluster;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.SecurityPlan;
import org.neo4j.driver.internal.bolt.api.values.Value;
import org.neo4j.driver.internal.bolt.routedimpl.ClusterCompositionLookupResult;
import org.neo4j.driver.internal.bolt.routedimpl.RoutingTable;

public interface RoutingTableHandler extends RoutingErrorHandler {
    Set<BoltServerAddress> servers();

    boolean isRoutingTableAged();

    CompletionStage<RoutingTable> ensureRoutingTable(
            SecurityPlan securityPlan,
            AccessMode mode,
            Set<String> rediscoveryBookmarks,
            Supplier<CompletionStage<Map<String, Value>>> authMapStageSupplier,
            BoltProtocolVersion minVersion);

    CompletionStage<RoutingTable> updateRoutingTable(ClusterCompositionLookupResult compositionLookupResult);

    RoutingTable routingTable();

    boolean staleRoutingTable(AccessMode mode);
}
