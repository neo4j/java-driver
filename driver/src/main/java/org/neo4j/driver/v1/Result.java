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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.v1;

import java.util.List;


/**
 * The result of running a statement, a stream of records represented as a cursor.
 *
 * The result cursor can be used to iterate over all the records in the stream and provide access
 * to their content.
 *
 * Results are valid until the next statement is run or until the end of the current transaction,
 * whichever comes first.
 *
 * Initially, before {@link #next()} has been called at least once, all field values are null.
 *
 * To keep a result around while further statements are run, or to use a result outside the scope
 * of the current transaction, see {@link #retain()}.
 */
public interface Result extends RecordLike, Resource
{
    /**
     * @return an immutable copy of the currently viewed record
     */
    ImmutableRecord record();

    /**
     * Retrieve the zero based position of the cursor in the stream of records.
     *
     * Initially, before {@link #next()} has been called at least once, the position is -1.
     *
     * @return the current position of the cursor
     */
    int position();

    /**
     * Count all records in this result.
     *
     * Calling this method exhausts the result cursor and moves it to the last record.
     *
     * @return the number of records in this result
     */
    int countRecords();

    /**
     * Test if the cursor is positioned at the last stream record of if the stream is empty.
     *
     * @return true if the cursor is at the last record or the stream is empty.
     */
    boolean atEnd();

    /**
     * Move to the next record in the result.
     *
     * @return true if there was another record, false if the stream is exhausted.
     */
    boolean next();

    /**
     * Advance the cursor as if calling next multiple times.
     *
     * @throws IllegalArgumentException if records is negative
     * @param records amount of records to be skipped
     * @return the actual number of records successfully skipped
     */
    int skip( int records );

    /**
     * Move to the first record if possible, otherwise do nothing.
     *
     * @return true if the cursor was successfully placed at the first record
     */
    boolean first();

    /**
     * Move to the first record if possible and verify that it is the only record.
     *
     * @return true if the cursor was successfully placed at the single first and only record
     */
    boolean single();

    /**
     * Retrieve and store the entire remaining result stream (including the current record).
     * This can be used if you want to iterate over the stream multiple times or to store the
     * whole result for later use.
     *
     * Calling this method exhausts the result cursor and moves it to the last record
     *
     * @return list of all remaining immutable records
     */
    List<ImmutableRecord> retain();

    /**
     * Summarize the result.
     *
     * Calling this method exhausts the result cursor and moves it to the last record.
     *
     * <pre class="doctest:ResultDocIT#summarizeUsage">
     * {@code
     * ResultSummary summary = session.run( "PROFILE MATCH (n:User {id: 12345}) RETURN n" ).summarize();
     * }
     * </pre>
     *
     * @return a summary for the whole query
     */
    ResultSummary summarize();
}
