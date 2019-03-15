/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.driver;

import java.util.concurrent.CompletionStage;

import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.reactive.RxSession;

/**
 * Accessor for a specific Neo4j graph database.
 * <p>
 * Driver implementations are typically thread-safe, act as a template
 * for session creation and host a connection pool. All configuration
 * and authentication settings are held immutably by the Driver. Should
 * different settings be required, a new Driver instance should be created.
 * <p>
 * A driver maintains a connection pool for each remote Neo4j server. Therefore
 * the most efficient way to make use of a Driver is to use the same instance
 * across the application.
 * <p>
 * To construct a new Driver, use one of the
 * {@link GraphDatabase#driver(String, AuthToken) GraphDatabase.driver} methods.
 * The <a href="https://tools.ietf.org/html/rfc3986">URI</a> passed to
 * this method determines the type of Driver created.
 * <br>
 * <table border="1" style="border-collapse: collapse">
 *     <caption>Available schemes and drivers</caption>
 *     <thead>
 *         <tr><th>URI Scheme</th><th>Driver</th></tr>
 *     </thead>
 *     <tbody>
 *         <tr>
 *             <td><code>bolt</code></td>
 *             <td>Direct driver: connects directly to the host and port specified in the URI.</td>
 *         </tr>
 *         <tr>
 *             <td><code>neo4j</code></td>
 *             <td>Routing driver: can automatically discover members of a Causal Cluster and route {@link Session sessions} based on {@link AccessMode}.</td>
 *         </tr>
 *     </tbody>
 * </table>
 *
 * @since 1.0 (Modified and Added {@link AsyncSession} and {@link RxSession} since 2.0)
 */
public interface Driver extends AutoCloseable
{
    /**
     * Return a flag to indicate whether or not encryption is used for this driver.
     *
     * @return true if the driver requires encryption, false otherwise
     */
    boolean isEncrypted();

    /**
     * Create a new general purpose {@link Session} with default {@link SessionParameters#empty() session parameters}.
     * <p>
     * Alias to {@link #session(SessionParameters)}}.
     *
     * @return a new {@link Session} object.
     */
    Session session();

    /**
     * Create a new {@link Session} for a specific database.
     * <p>
     * Alias to {@link #session(SessionParameters)}}.
     *
     * @param databaseName the database the session connects to.
     * @return a new {@link Session} object.
     */
    Session session( String databaseName );

    /**
     * Create a new {@link Session} for a specific type of work on the default database.
     * <p>
     * Alias to {@link #session(SessionParameters)}}.
     *
     * @param mode the type of access required by units of work in this session,
     * e.g. {@link AccessMode#READ read access} or {@link AccessMode#WRITE write access}.
     * @return a new {@link Session} object.
     */
    Session session( AccessMode mode );

    /**
     * Create a new {@link Session} towards a specific database for a specific type of work.
     *<p>
     * Alias to {@link #session(SessionParameters)}}.
     *
     * @param databaseName the database the session connects to.
     * @param mode the type of access required by units of work in this session,
     * e.g. {@link AccessMode#READ read access} or {@link AccessMode#WRITE write access}.
     *
     * @return a new {@link Session} object.
     */
    Session session( String databaseName, AccessMode mode );

    /**
     * Create a new {@link Session} with a specified {@link SessionParameters}.
     * @param parameters the session parameters which specifies the attribute of the session.
     * @return a new {@link Session} object.
     * @see SessionParameters
     */
    Session session( SessionParameters parameters );

    /**
     * Close all the resources assigned to this driver, including open connections and IO threads.
     * <p>
     * This operation works the same way as {@link #closeAsync()} but blocks until all resources are closed.
     */
    @Override
    void close();

    /**
     * Close all the resources assigned to this driver, including open connections and IO threads.
     * <p>
     * This operation is asynchronous and returns a {@link CompletionStage}. This stage is completed with
     * {@code null} when all resources are closed. It is completed exceptionally if termination fails.
     *
     * @return a {@link CompletionStage completion stage} that represents the asynchronous close.
     */
    CompletionStage<Void> closeAsync();

    /**
     * Returns the driver metrics if metrics reporting is enabled via {@link Config.ConfigBuilder#withDriverMetrics()}.
     * Otherwise a {@link ClientException} will be thrown.
     * @return the driver metrics if enabled.
     * @throws ClientException if the driver metrics reporting is not enabled.
     */
    Metrics metrics();

    /**
     * Create a new general purpose {@link RxSession} with default {@link SessionParameters#empty() session parameters}.
     * The {@link RxSession} provides a reactive way to run queries and process results.
     * <p>
     * Alias to {@link #rxSession(SessionParameters)}}.
     *
     * @return @return a new {@link RxSession} object.
     */
    RxSession rxSession();

    /**
     * Create a new {@link RxSession} for a specific database.
     * The {@link RxSession} provides a reactive way to run queries and process results.
     * <p>
     * Alias to {@link #rxSession(SessionParameters)}}.
     *
     * @param databaseName the database the session connects to.
     * @return a new {@link RxSession} object.
     */
    RxSession rxSession( String databaseName );
    /**
     * Create a new {@link RxSession} for a specific type of work on the default database.
     * The {@link RxSession} provides a reactive way to run queries and process results.
     * <p>
     * Alias to {@link #rxSession(SessionParameters)}}.
     *
     * @param mode the type of access required by units of work in this session,
     * e.g. {@link AccessMode#READ read access} or {@link AccessMode#WRITE write access}.
     * @return a new {@link RxSession} object.
     */
    RxSession rxSession( AccessMode mode );

    /**
     * Create a new {@link RxSession} towards a specific database for a specific type of work.
     * The {@link RxSession} provides a reactive way to run queries and process results.
     *<p>
     * Alias to {@link #rxSession(SessionParameters)}}.
     *
     * @param databaseName the database the session connects to.
     * @param mode the type of access required by units of work in this session,
     * e.g. {@link AccessMode#READ read access} or {@link AccessMode#WRITE write access}.
     *
     * @return a new {@link RxSession} object.
     */
    RxSession rxSession( String databaseName, AccessMode mode );

    /**
     * Create a new {@link RxSession} with specified {@link SessionParameters}.
     * The {@link RxSession} provides a reactive way to run queries and process results.
     *
     * @param parameters the session parameters which specifies the attribute of the session.
     * @return a new {@link RxSession} object.
     */
    RxSession rxSession( SessionParameters parameters );

    /**
     * Create a new general purpose {@link AsyncSession} with default {@link SessionParameters#empty() session parameters}.
     * The {@link AsyncSession} provides an asynchronous way to run queries and process results.
     * <p>
     * Alias to {@link #asyncSession(SessionParameters)}}.
     *
     * @return @return a new {@link AsyncSession} object.
     */
    AsyncSession asyncSession();

    /**
     * Create a new {@link AsyncSession} for a specific database.
     * The {@link AsyncSession} provides an asynchronous way to run queries and process results.
     * <p>
     * Alias to {@link #asyncSession(SessionParameters)}}.
     *
     * @param databaseName the database the session connects to.
     * @return a new {@link AsyncSession} object.
     */
    AsyncSession asyncSession( String databaseName );
    /**
     * Create a new {@link AsyncSession} for a specific type of work on the default database.
     * The {@link AsyncSession} provides an asynchronous way to run queries and process results.
     * <p>
     * Alias to {@link #asyncSession(SessionParameters)}}.
     *
     * @param mode the type of access required by units of work in this session,
     * e.g. {@link AccessMode#READ read access} or {@link AccessMode#WRITE write access}.
     * @return a new {@link AsyncSession} object.
     */
    AsyncSession asyncSession( AccessMode mode );
    /**
     * Create a new {@link AsyncSession} towards a specific database for a specific type of work.
     * The {@link AsyncSession} provides an asynchronous way to run queries and process results.
     *<p>
     * Alias to {@link #asyncSession(SessionParameters)}}.
     *
     * @param databaseName the database the session connects to.
     * @param mode the type of access required by units of work in this session,
     * e.g. {@link AccessMode#READ read access} or {@link AccessMode#WRITE write access}.
     *
     * @return a new {@link AsyncSession} object.
     */
    AsyncSession asyncSession( String databaseName, AccessMode mode );
    /**
     * Create a new {@link AsyncSession} with specified {@link SessionParameters}.
     * The {@link AsyncSession} provides an asynchronous way to run queries and process results.
     *
     * @param parameters the session parameters which specifies the attribute of the session.
     * @return a new {@link AsyncSession} object.
     */
    AsyncSession asyncSession( SessionParameters parameters );
}
