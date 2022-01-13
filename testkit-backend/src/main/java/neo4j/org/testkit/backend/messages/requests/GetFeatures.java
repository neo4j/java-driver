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
package neo4j.org.testkit.backend.messages.requests;

import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.FeatureList;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Setter
@Getter
public class GetFeatures implements TestkitRequest
{
    private static final Set<String> COMMON_FEATURES = new HashSet<>( Arrays.asList(
            "Feature:Bolt:4.0",
            "Feature:Bolt:4.1",
            "Feature:Bolt:4.2",
            "Feature:Bolt:4.3",
            "Feature:Bolt:4.4",
            "AuthorizationExpiredTreatment",
            "ConfHint:connection.recv_timeout_seconds",
            "Temporary:DriverFetchSize",
            "Temporary:DriverMaxTxRetryTime",
            "Feature:Auth:Bearer",
            "Feature:Auth:Kerberos",
            "Feature:Auth:Custom",
            "Feature:Impersonation",
            "Temporary:FastFailingDiscovery",
            "Feature:TLS:1.1",
            "Feature:TLS:1.2",
            "Feature:API:SSLSchemes",
            "Feature:API:Liveness.Check",
            "Temporary:DriverMaxConnectionPoolSize",
            "Temporary:ConnectionAcquisitionTimeout",
            "Temporary:GetConnectionPoolMetrics",
            "Temporary:CypherPathAndRelationship"
    ) );

    private static final Set<String> SYNC_FEATURES = new HashSet<>( Arrays.asList(
            "Feature:Bolt:3.0",
            "Optimization:PullPipelining",
            "Temporary:TransactionClose",
            "Feature:API:Result.List",
            "Optimization:ResultListFetchAll"
    ) );

    private static final Set<String> ASYNC_FEATURES = new HashSet<>( Arrays.asList(
            "Feature:Bolt:3.0",
            "Optimization:PullPipelining",
            "Feature:API:Result.List",
            "Optimization:ResultListFetchAll"
    ) );

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        Set<String> features = new HashSet<>( COMMON_FEATURES );
        features.addAll( SYNC_FEATURES );
        return createResponse( features );
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync( TestkitState testkitState )
    {
        Set<String> features = new HashSet<>( COMMON_FEATURES );
        features.addAll( ASYNC_FEATURES );
        return CompletableFuture.completedFuture( createResponse( features ) );
    }

    @Override
    public Mono<TestkitResponse> processRx( TestkitState testkitState )
    {
        return Mono.just( createResponse( COMMON_FEATURES ) );
    }

    private FeatureList createResponse( Set<String> features )
    {
        return FeatureList.builder().data( FeatureList.FeatureListBody.builder().features( features ).build() ).build();
    }
}
