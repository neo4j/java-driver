/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.driver.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.StandardSocketOptions;
import java.net.URI;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.internal.BoltServerAddress;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.logging.Level.INFO;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.driver.AuthTokens.basic;
import static org.neo4j.driver.Logging.console;
import static org.neo4j.driver.util.FileTools.updateProperties;
import static org.neo4j.driver.util.Neo4jSettings.CURRENT_BOLT_PORT;
import static org.neo4j.driver.util.Neo4jSettings.CURRENT_HTTP_PORT;
import static org.neo4j.driver.util.Neo4jSettings.TEST_JVM_ID;
import static org.neo4j.driver.util.Neo4jSettings.WINDOWS_SERVICE_NAME;
import static org.neo4j.driver.util.cc.CommandLineUtil.boltKitAvailable;
import static org.neo4j.driver.util.cc.CommandLineUtil.executeCommand;

/**
 * This class wraps the neo4j stand-alone jar in some code to help pulling it in from a remote URL and then launching
 * it in a separate process.
 */
public class Neo4jRunner
{
    private static Neo4jRunner globalInstance;

    private static final String DEFAULT_NEOCTRL_ARGS = "-e 3.5.11";
    private static final String ENV_NEOCTRL_ARGS = System.getenv( "JAVA_DRIVER_NEOCTRL_ARGS" );
    public static final String NEOCTRL_ARGS = System.getProperty( "neoctrl.args", ENV_NEOCTRL_ARGS == null ? DEFAULT_NEOCTRL_ARGS : ENV_NEOCTRL_ARGS );
    public static final Config DEFAULT_CONFIG = Config.builder().withLogging( console( INFO ) ).withoutEncryption().build();

    public static final String USER = "neo4j";
    public static final String PASSWORD = "password";
    public static final AuthToken DEFAULT_AUTH_TOKEN = basic( USER, PASSWORD );

    private Neo4jSettings currentSettings = Neo4jSettings.TEST_SETTINGS;

    public static final String TARGET_DIR = new File( "../target" ).getAbsolutePath();
    private static final String NEO4J_DIR = new File( TARGET_DIR, "test-server-" + TEST_JVM_ID ).getAbsolutePath();
    private static final String NEO4J_HOME = "neo4jHome";
    public static final String HOME_DIR = new File( NEO4J_DIR, NEO4J_HOME ).getAbsolutePath();

    private Driver driver;
    private boolean restartDriver;

    public int httpPort()
    {
        return CURRENT_HTTP_PORT;
    }

    public int boltPort()
    {
        return CURRENT_BOLT_PORT;
    }

    public BoltServerAddress boltAddress()
    {
        return new BoltServerAddress( boltUri() );
    }

    public URI boltUri()
    {
        return URI.create( "bolt://localhost:" + boltPort() );
    }

    /** Global runner controlling a single server, used to avoid having to restart the server between tests */
    public static synchronized Neo4jRunner getOrCreateGlobalRunner() throws IOException
    {
        assumeTrue( boltKitAvailable(), "BoltKit support unavailable" );
        if ( globalInstance == null )
        {
            globalInstance = new Neo4jRunner();
        }
        return globalInstance;
    }

    public static synchronized boolean globalRunnerExists()
    {
        return globalInstance != null;
    }

    private Neo4jRunner() throws IOException
    {
        try
        {
            installNeo4j();
            updateServerSettingsFile();
            startNeo4j();
        }
        finally
        {
            // Make sure we stop on JVM exit even if start failed
            installShutdownHook();
        }
    }

    public void ensureRunning( Neo4jSettings neo4jSettings )
    {
        ServerStatus status = serverStatus();
        switch( status )
        {
        case OFFLINE:
            updateServerSettings( neo4jSettings );
            startNeo4j();
            break;
        case ONLINE:
            restartNeo4j( neo4jSettings );
            break;
        }
    }

    public Driver driver()
    {
        if ( restartDriver )
        {
            restartDriver = false;
            if ( driver != null )
            {
                driver.close();
                driver = null;
            }
        }

        if ( driver == null )
        {
            driver = GraphDatabase.driver( boltUri(), DEFAULT_AUTH_TOKEN, DEFAULT_CONFIG );
        }
        return driver;
    }

    private void installNeo4j() throws IOException
    {
        File targetHomeFile = new File( HOME_DIR );
        if( targetHomeFile.exists() )
        {
            executeCommand( "neoctrl-install-service", HOME_DIR );
            debug( "Found and using server installed at `%s`. ", HOME_DIR );
        }
        else
        {
            List<String> commands = new ArrayList<>();
            commands.add( "neoctrl-install" );
            String[] split = NEOCTRL_ARGS.trim().split( "\\s+" );
            commands.addAll( asList( split ) );
            commands.add( NEO4J_DIR );
            commands.add( WINDOWS_SERVICE_NAME );

            String tempHomeDir = executeCommand( commands ).trim();

            if ( !Objects.equals( NEO4J_HOME, new File( tempHomeDir ).getName() ) )
            {
                throw new IOException( format( "Expecting server installed at %s, but installed at %s", HOME_DIR, tempHomeDir ) );
            }

            debug( "Installed server at `%s`.", HOME_DIR );
            executeCommand( "neoctrl-create-user", HOME_DIR, USER, PASSWORD );
        }
    }

    private void uninstallNeo4j()
    {
        debug( "Uninstalling server..." );
        executeCommand( "neoctrl-uninstall", HOME_DIR );
        debug( "Uninstalled server." );
    }

    public void startNeo4j()
    {
        debug( "Starting server..." );
        executeCommand( "neoctrl-start", HOME_DIR );
        debug( "Server started." );
    }

    public synchronized void stopNeo4j()
    {
        if( serverStatus() == ServerStatus.OFFLINE )
        {
            return;
        }
        restartDriver = true;

        debug( "Stopping server..." );
        executeCommand( "neoctrl-stop", HOME_DIR );
        debug( "Server stopped." );
    }

    public void killNeo4j()
    {
        if ( serverStatus() == ServerStatus.OFFLINE )
        {
            return;
        }
        restartDriver = true;

        debug( "Killing server..." );
        executeCommand( "neoctrl-stop", "-k", HOME_DIR );
        debug( "Server killed." );
    }

    public void forceToRestart()
    {
        stopNeo4j();
        startNeo4j();
    }

    /**
     * Will only restart the server if any configuration changes happens
     * @param neo4jSettings
     */
    public void restartNeo4j( Neo4jSettings neo4jSettings )
    {
        if( updateServerSettings( neo4jSettings ) ) // needs to update server setting files
        {
            forceToRestart();
        }
    }

    /**
     * prints the debug log contents to stdOut
     */
    public void dumpDebugLog()
    {
        try
        {
            System.out.println( "Debug log for: " + HOME_DIR );
            Scanner input = new Scanner( new File( HOME_DIR + "/logs/debug.log" ));

            while (input.hasNextLine())
            {
                System.out.println(input.nextLine());
            }
        }
        catch ( FileNotFoundException e )
        {
            System.out.println("Unable to find debug log file for: " + HOME_DIR);
            e.printStackTrace();
        }

    }

    private enum ServerStatus
    {
        ONLINE, OFFLINE
    }

    private ServerStatus serverStatus()
    {
        try
        {
            SocketChannel soChannel = SocketChannel.open();
            soChannel.setOption( StandardSocketOptions.SO_REUSEADDR, true );
            soChannel.connect( boltAddress().toSocketAddress() );
            soChannel.close();
            return ServerStatus.ONLINE;
        }
        catch ( IOException e )
        {
            return ServerStatus.OFFLINE;
        }
    }

    private boolean updateServerSettings( Neo4jSettings newSetting )
    {
        if ( currentSettings.equals( newSetting ) )
        {
            return false;
        }
        else
        {
            currentSettings = newSetting;
        }
        updateServerSettingsFile();
        return true;
    }

    /**
     * Write updated neo4j settings into neo4j-server.properties for use by the next start
     */
    private void updateServerSettingsFile()
    {
        Map<String, String> propertiesMap = currentSettings.propertiesMap();

        if ( propertiesMap.isEmpty() )
        {
            return;
        }

        File oldFile = new File( HOME_DIR, "conf/neo4j.conf" );
        try
        {
            debug( "Changing server properties file (for next start): %s", oldFile.getCanonicalPath() );
            for ( Map.Entry<String, String> property : propertiesMap.entrySet() )
            {
                String name = property.getKey();
                Object value = property.getValue();
                debug( "%s=%s", name, value );
            }

            updateProperties( oldFile, propertiesMap, currentSettings.excludes() );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private void installShutdownHook()
    {
        Runtime.getRuntime().addShutdownHook( new Thread( () ->
        {
            try
            {
                debug( "Starting shutdown hook" );
                if ( driver != null )
                {
                    driver.close();
                }
                stopNeo4j();
                uninstallNeo4j();
                debug( "Finished shutdown hook" );
            }
            catch ( Exception e )
            {
                e.printStackTrace();
            }
        } ) );
    }

    public static void debug( String text, Object... args )
    {
        System.out.println( format( text, args ) );
    }
}

