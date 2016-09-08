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
package org.neo4j.driver.v1.integration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLHandshakeException;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.neo4j.driver.internal.logging.DevNullLogger;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.security.TLSSocketChannel;
import org.neo4j.driver.internal.util.CertificateTool;
import org.neo4j.driver.testing.TestNeo4j;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.util.CertificateToolTest;
import org.neo4j.driver.v1.util.Neo4jSettings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import static org.neo4j.driver.internal.security.TrustOnFirstUseTrustManager.fingerprint;

public class TLSSocketChannelIT
{
    @Rule
    public TestNeo4j neo4j = new TestNeo4j();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(  );

    @BeforeClass
    public static void setup() throws IOException, InterruptedException
    {
        /* uncomment for JSSE debugging info */
//         System.setProperty( "javax.net.debug", "all" );
    }

    @Test
    public void shouldPerformTLSHandshakeWithEmptyKnownCertsFile() throws Throwable
    {
        File knownCerts = File.createTempFile( "neo4j_known_hosts", ".tmp" );
        knownCerts.deleteOnExit();

        performTLSHandshakeUsingKnownCerts( knownCerts );
    }

    private void performTLSHandshakeUsingKnownCerts( File knownCerts ) throws Throwable
    {
        // Given
        Logger logger = mock( Logger.class );
        BoltServerAddress address = BoltServerAddress.LOCAL_DEFAULT;
        SocketChannel channel = SocketChannel.open();
        channel.connect( address.toSocketAddress() );

        // When

        SecurityPlan securityPlan = SecurityPlan.forTrustOnFirstUse( knownCerts, address, new DevNullLogger() );
        TLSSocketChannel sslChannel =
                new TLSSocketChannel( address, securityPlan, channel, logger );
        sslChannel.close();

        // Then
        verify( logger, atLeastOnce() ).debug( "~~ [CLOSED SECURE CHANNEL]" );
    }

    @Test
    public void shouldPerformTLSHandshakeWithTrustedCert() throws Throwable
    {
        try
        {
            // Given
            BoltServerAddress address = BoltServerAddress.LOCAL_DEFAULT;
            // Create root certificate
            File rootCert = folder.newFile( "temp_root_cert.cert" );
            File rootKey = folder.newFile( "temp_root_key.key" );

            CertificateToolTest.SelfSignedCertificateGenerator
                    certGenerator = new CertificateToolTest.SelfSignedCertificateGenerator();
            certGenerator.saveSelfSignedCertificate( rootCert );
            certGenerator.savePrivateKey( rootKey );

            // Generate certificate signing request and get a certificate signed by the root private key
            File cert = folder.newFile( "temp_cert.cert" );
            File key = folder.newFile( "temp_key.key" );
            CertificateToolTest.CertificateSigningRequestGenerator
                    csrGenerator = new CertificateToolTest.CertificateSigningRequestGenerator();
            X509Certificate signedCert = certGenerator.sign(
                    csrGenerator.certificateSigningRequest(), csrGenerator.publicKey() );
            csrGenerator.savePrivateKey( key );
            CertificateTool.saveX509Cert( signedCert, cert );

            // Give the server certs to database
            neo4j.updateEncryptionKeyAndCert( key, cert );

            Logger logger = mock( Logger.class );
            SocketChannel channel = SocketChannel.open();
            channel.connect( address.toSocketAddress() );

            // When
            SecurityPlan securityPlan = SecurityPlan.forSignedCertificates( rootCert );
            TLSSocketChannel sslChannel =
                    new TLSSocketChannel( address, securityPlan, channel, logger
                    );
            sslChannel.close();

            // Then
            verify( logger, atLeastOnce() ).debug( "~~ [OPENING SECURE CHANNEL]" );
        }
        finally
        {
            // always restore the db default settings
            neo4j.restart();
        }
    }

    @Test
    public void shouldFailTLSHandshakeDueToWrongCertInKnownCertsFile() throws Throwable
    {
        // Given
        BoltServerAddress address = BoltServerAddress.LOCAL_DEFAULT;
        SocketChannel channel = SocketChannel.open();
        channel.connect( address.toSocketAddress() );
        File knownCerts = File.createTempFile( "neo4j_known_hosts", ".tmp" );
        knownCerts.deleteOnExit();

        //create a Fake Cert for the server in knownCert
        createFakeServerCertPairInKnownCerts( address, knownCerts );

        // When & Then
        SecurityPlan securityPlan = SecurityPlan.forTrustOnFirstUse( knownCerts, address, new DevNullLogger() );
        TLSSocketChannel sslChannel = null;
        try
        {
            sslChannel = new TLSSocketChannel( address, securityPlan, channel, mock( Logger.class ) );
            sslChannel.close();
        }
        catch ( SSLHandshakeException e )
        {
            assertEquals( "General SSLEngine problem", e.getMessage() );
            assertEquals( "General SSLEngine problem", e.getCause().getMessage() );
            assertTrue( e.getCause().getCause().getMessage().contains(
                    "If you trust the certificate the server uses now, simply remove the line that starts with" ) );
        }
        finally
        {
            if ( sslChannel != null )
            {
                sslChannel.close();
            }
        }
    }

    private void createFakeServerCertPairInKnownCerts( BoltServerAddress address, File knownCerts )
            throws Throwable
    {
        address = address.resolve();  // localhost -> 127.0.0.1
        String serverId = address.toString();

        X509Certificate cert = CertificateToolTest.generateSelfSignedCertificate();
        String certStr = fingerprint(cert);

        BufferedWriter writer = new BufferedWriter( new FileWriter( knownCerts, true ) );
        writer.write( serverId + "," + certStr );
        writer.newLine();
        writer.close();
    }

    @Test
    public void shouldFailTLSHandshakeDueToServerCertNotSignedByKnownCA() throws Throwable
    {
        // Given
        neo4j.restart(
                Neo4jSettings.TEST_SETTINGS.updateWith(
                        Neo4jSettings.CERT_DIR,
                        folder.getRoot().getAbsolutePath().replace("\\", "/") ) );
        SocketChannel channel = SocketChannel.open();
        channel.connect( neo4j.address().toSocketAddress() );
        File trustedCertFile = folder.newFile( "neo4j_trusted_cert.tmp" );
        X509Certificate aRandomCert = CertificateToolTest.generateSelfSignedCertificate();
        CertificateTool.saveX509Cert( aRandomCert, trustedCertFile );

        // When & Then
        SecurityPlan securityPlan = SecurityPlan.forSignedCertificates( trustedCertFile );
        TLSSocketChannel sslChannel = null;
        try
        {
            sslChannel = new TLSSocketChannel( neo4j.address(), securityPlan, channel, mock( Logger.class ) );
            sslChannel.close();
        }
        catch ( SSLHandshakeException e )
        {
            assertEquals( "General SSLEngine problem", e.getMessage() );
            assertEquals( "General SSLEngine problem", e.getCause().getMessage() );
            assertEquals( "No trusted certificate found", e.getCause().getCause().getMessage() );
        }
        finally
        {
            if ( sslChannel != null )
            {
                sslChannel.close();
            }
        }
    }

    @Test
    public void shouldPerformTLSHandshakeWithTheSameTrustedServerCert() throws Throwable
    {
        BoltServerAddress address = BoltServerAddress.LOCAL_DEFAULT;
        Logger logger = mock( Logger.class );
        SocketChannel channel = SocketChannel.open();
        channel.connect( address.toSocketAddress() );

        // When
        SecurityPlan securityPlan = SecurityPlan.forSignedCertificates( neo4j.certFile() );
        TLSSocketChannel sslChannel = new TLSSocketChannel( address, securityPlan, channel, logger );
        sslChannel.close();

        // Then
        verify( logger, atLeastOnce() ).debug( "~~ [OPENING SECURE CHANNEL]" );
    }

    @Test
    public void shouldEstablishTLSConnection() throws Throwable
    {

        Config config = Config.build().withEncryptionLevel( Config.EncryptionLevel.REQUIRED ).toConfig();

        try( Driver driver = GraphDatabase.driver( neo4j.boltURI(), config );
             Session session = driver.session() )
        {
            StatementResult result = session.run( "RETURN 1" );
            assertEquals( 1, result.next().get( 0 ).asInt() );
            assertFalse( result.hasNext() );
        }
    }
}
