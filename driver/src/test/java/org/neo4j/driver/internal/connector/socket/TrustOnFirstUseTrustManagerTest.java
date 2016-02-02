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
package org.neo4j.driver.internal.connector.socket;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.PrintWriter;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Scanner;

import org.neo4j.driver.internal.spi.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.connector.socket.TrustOnFirstUseTrustManager.fingerprint;

public class TrustOnFirstUseTrustManagerTest
{
    private File knownCertsFile;

    private String knownServerIp;
    private int knownServerPort;
    private String knownServer;

    @Rule
    public TemporaryFolder testDir = new TemporaryFolder();
    private X509Certificate knownCertificate;

    @Before
    public void setup() throws Throwable
    {
        // create the cert file with one ip:port and some random "cert" in it
        knownCertsFile = testDir.newFile();
        knownServerIp = "1.2.3.4";
        knownServerPort = 100;
        knownServer = knownServerIp + ":" + knownServerPort;

        knownCertificate = mock( X509Certificate.class );
        when( knownCertificate.getEncoded() ).thenReturn( "certificate".getBytes( "UTF-8" ) );

        PrintWriter writer = new PrintWriter( knownCertsFile );
        writer.println( " # I am a comment." );
        writer.println( knownServer + " " + fingerprint( knownCertificate ) );
        writer.close();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @After
    public void teardown()
    {
        knownCertsFile.delete();
    }

    @Test
    public void shouldLoadExistingCert() throws Throwable
    {
        // Given
        Logger logger = mock(Logger.class);
        TrustOnFirstUseTrustManager manager =
                new TrustOnFirstUseTrustManager( knownServerIp, knownServerPort, knownCertsFile, logger );

        X509Certificate wrongCertificate = mock( X509Certificate.class );
        when( wrongCertificate.getEncoded() ).thenReturn( "fake certificate".getBytes() );

        // When & Then
        try
        {
            manager.checkServerTrusted( new X509Certificate[]{wrongCertificate}, null );
            fail( "Should not trust the fake certificate" );
        }
        catch ( CertificateException e )
        {
            assertTrue( e.getMessage().contains(
                    "If you trust the certificate the server uses now, simply remove the line that starts with" ) );
            verifyNoMoreInteractions( logger );
        }
    }

    @Test
    public void shouldSaveNewCert() throws Throwable
    {
        // Given
        int newPort = 200;
        Logger logger = mock(Logger.class);
        TrustOnFirstUseTrustManager manager = new TrustOnFirstUseTrustManager( knownServerIp, newPort, knownCertsFile, logger );

        String fingerprint = fingerprint( knownCertificate );

        // When
        manager.checkServerTrusted( new X509Certificate[]{knownCertificate}, null );

        // Then no exception should've been thrown, and we should've logged that we now trust this certificate
        verify(logger).warn( "Adding %s as known and trusted certificate for %s.", fingerprint, "1.2.3.4:200" );

        // And the file should contain the right info
        Scanner reader = new Scanner( knownCertsFile );

        String line;
        line = nextLine( reader );
        assertEquals( knownServer + " " + fingerprint, line );
        assertTrue( reader.hasNextLine() );
        line = nextLine( reader );
        assertEquals( knownServerIp + ":" + newPort + " " + fingerprint, line );
    }

    private String nextLine( Scanner reader )
    {
        String line;
        do
        {
            assertTrue( reader.hasNext() );
            line = reader.nextLine();
        }
        while ( line.trim().startsWith( "#" ) );
        return line;
    }
}
