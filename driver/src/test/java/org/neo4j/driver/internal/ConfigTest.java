/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.driver.internal;

import org.junit.Test;

import java.io.File;

import org.neo4j.driver.v1.Config;

import static org.junit.Assert.assertEquals;

public class ConfigTest
{
    private static final File DEFAULT_KNOWN_CERTS = new File( System.getProperty( "user.home" ), ".neo4j/neo4j_known_certs" );

    @Test
    public void shouldDefaultToKnownCerts()
    {
        // Given
        Config config = Config.defaultConfig();

        // When
        Config.TrustStrategy authConfig = config.trustStrategy();

        // Then
        assertEquals( authConfig.strategy(), Config.TrustStrategy.Strategy.TRUST_ON_FIRST_USE );
        assertEquals( DEFAULT_KNOWN_CERTS.getAbsolutePath(), authConfig.certFile().getAbsolutePath() );
    }

    @Test
    public void shouldChangeToNewKnownCerts()
    {
        // Given
        File knownCerts = new File( "new_known_certs" );
        Config config = Config.build().withTrustStrategy( Config.TrustStrategy.trustOnFirstUse( knownCerts ) ).toConfig();

        // When
        Config.TrustStrategy authConfig = config.trustStrategy();

        // Then
        assertEquals( authConfig.strategy(), Config.TrustStrategy.Strategy.TRUST_ON_FIRST_USE );
        assertEquals( knownCerts.getAbsolutePath(), authConfig.certFile().getAbsolutePath() );
    }

    @Test
    public void shouldChangeToTrustedCert()
    {
        // Given
        File trustedCert = new File( "trusted_cert" );
        Config config = Config.build().withTrustStrategy( Config.TrustStrategy.trustSignedBy( trustedCert ) ).toConfig();

        // When
        Config.TrustStrategy authConfig = config.trustStrategy();

        // Then
        assertEquals( authConfig.strategy(), Config.TrustStrategy.Strategy.TRUST_SIGNED_CERTIFICATES );
        assertEquals( trustedCert.getAbsolutePath(), authConfig.certFile().getAbsolutePath() );
    }

    public static void deleteDefaultKnownCertFileIfExists()
    {
        if( DEFAULT_KNOWN_CERTS.exists() )
        {
            DEFAULT_KNOWN_CERTS.delete();
        }
    }

}
