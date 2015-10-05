/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.util;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.neo4j.driver.Config;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.connector.socket.SocketClient;
import org.neo4j.driver.internal.logging.DevNullLogger;

import static junit.framework.TestCase.assertFalse;
import static org.neo4j.driver.internal.ConfigTest.deleteDefaultKnownCertFileIfExists;
import static org.neo4j.driver.util.FileTools.deleteRecursively;
import static org.neo4j.driver.util.FileTools.setProperty;

/**
 * This class wraps the neo4j stand-alone jar in some code to help pulling it in from a remote URL and then launching
 * it in a separate process.
 */
public class Neo4jRunner
{
    public static final String DEFAULT_URL = "bolt://localhost:7687";

    private static Neo4jRunner globalInstance;
    private static boolean externalServer = Boolean.getBoolean( "neo4j.useExternalServer" );
    private static boolean shutdownHookRegistered = false;

    private static final String neo4jVersion = System.getProperty( "version", "3.0.0-alpha" );
    private static final String neo4jLink = System.getProperty( "packageUri",
            String.format( "http://alpha.neohq.net/dist/neo4j-enterprise-" +
                           "%s-unix.tar.gz", neo4jVersion ) );

    private final File neo4jDir = new File( "./target/neo4j" );
    private final File neo4jHome = new File( neo4jDir, neo4jVersion );
    private final File dataDir = new File( neo4jHome, "data" );
    private boolean isTLSEnabled;


    public static void main( String... args ) throws Exception
    {
        Neo4jRunner neo4jRunner = new Neo4jRunner();
        neo4jRunner.startServer();
        neo4jRunner.stopServer();
    }

    /** Global runner controlling a single server, used to avoid having to restart the server between tests */
    public static synchronized Neo4jRunner getOrCreateGlobalServer() throws IOException, InterruptedException
    {
        if ( globalInstance == null )
        {
            globalInstance = new Neo4jRunner();
            globalInstance.startServer();
        }
        return globalInstance;
    }

    public Neo4jRunner() throws IOException
    {
        if ( canControlServer() && !neo4jHome.exists() )
        {
            // download neo4j server from a URL
            File neo4jTarball = new File( "./target/" + neo4jVersion + ".tar.gz" );
            ensureDownloaded( neo4jTarball, neo4jLink );

            // Untar the neo4j server
            System.out.println( "Extracting: " + neo4jTarball + " -> " + neo4jDir );
            FileTools.extractTarball( neo4jTarball, neo4jDir, neo4jHome );
        }
    }

    private void ensureDownloaded( File file, String downloadLink ) throws IOException
    {
        if ( file.exists() )
        {
            file.delete();
        }
        file.getParentFile().mkdirs();
        System.out.println( "Copying: " + downloadLink + " -> " + file );
        FileTools.streamFileTo( downloadLink, file );

    }

    public void startServer() throws IOException, InterruptedException
    {
        if ( canControlServer() )
        {
            assertFalse( "A server instance is already running", serverResponds() );

            deleteRecursively( new File( dataDir, "graph.db" ) );
            deleteDefaultKnownCertFileIfExists();

            Process process = runNeo4j( "start" );
            stopOnExit();

            awaitServerResponds( process );
        }
    }

    public Process runNeo4j( String cmd ) throws IOException
    {
        File startScript = new File( neo4jHome, "bin/neo4j" );
        startScript.setExecutable( true );
        return new ProcessBuilder().inheritIO().command( startScript.getAbsolutePath(), cmd ).start();
    }

    public void stopServer() throws IOException, InterruptedException
    {
        if ( canControlServer() )
        {
            runNeo4j( "stop" ).waitFor();
        }
    }

    public void enableTLS( boolean isTLSEnabled )
    {
        this.isTLSEnabled = isTLSEnabled;
        setServerProperty( "dbms.bolt.tls.enabled", String.valueOf( isTLSEnabled ) );
    }

    /**
     * Write the new property and its value in neo4j-server.properties.
     * If the server is already running, then stop and restart the server to reload the changes in the property file
     * @param name
     * @param value
     */
    private void setServerProperty( String name, String value )
    {
        File oldFile = new File( neo4jHome, "conf/neo4j-server.properties" );
        try
        {
            setProperty( oldFile, name, value );

            System.out.println( "Restart server to reload property change: " + name + "=" + value );
            this.stopServer();
            this.startServer();
        }
        catch ( Exception e )
        {
            System.out.println( "Failed to change property." );
            throw new RuntimeException( e );
        }
    }

    public boolean canControlServer()
    {
        return !externalServer;
    }

    private void awaitServerResponds( Process process ) throws IOException, InterruptedException
    {
        long timeout = System.currentTimeMillis() + 1000 * 30;
        for (; ; )
        {
            process.waitFor();
            if ( serverResponds() )
            {
                return;
            }
            else
            {
                Thread.sleep( 100 );
            }

            if ( System.currentTimeMillis() > timeout )
            {
                throw new RuntimeException( "Waited for 30 seconds for server to respond to socket calls, " +
                                            "but no response, timing out to avoid blocking forever." );
            }
        }
    }

    private boolean serverResponds() throws IOException, InterruptedException
    {
        try
        {
            URI uri = URI.create( DEFAULT_URL );
            Config config = Config.defaultConfig();
            if( isTLSEnabled )
            {
                config = Config.build().withTlsEnabled( true ).toConfig();
            }
            SocketClient client = new SocketClient( uri.getHost(), uri.getPort(),
                    config, new DevNullLogger() );
            client.start();
            client.stop();
            return true;
        }
        catch ( ClientException e )
        {
            return false;
        }
    }

    private void stopOnExit()
    {
        if( !shutdownHookRegistered )
        {
            Runtime.getRuntime().addShutdownHook( new Thread( new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        stopServer();
                    }
                    catch ( Exception e )
                    {
                        // cannot help you anything sorry
                        e.printStackTrace();
                    }
                }
            } ) );
            shutdownHookRegistered = true;
        }
    }
}
