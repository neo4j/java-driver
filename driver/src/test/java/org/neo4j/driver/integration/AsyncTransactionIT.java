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
package org.neo4j.driver.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.Record;
import org.neo4j.driver.Statement;
import org.neo4j.driver.Value;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.StatementResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.StatementType;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.util.Iterables.single;
import static org.neo4j.driver.internal.util.Matchers.containsResultAvailableAfterAndResultConsumedAfter;
import static org.neo4j.driver.internal.util.Matchers.syntaxError;
import static org.neo4j.driver.util.TestUtil.await;

@ParallelizableIT
class AsyncTransactionIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private AsyncSession session;

    @BeforeEach
    void setUp()
    {
        session = neo4j.driver().asyncSession();
    }

    @AfterEach
    void tearDown()
    {
        session.closeAsync();
    }

    @Test
    void shouldBePossibleToCommitEmptyTx()
    {
        String bookmarkBefore = session.lastBookmark();

        AsyncTransaction tx = await( session.beginTransactionAsync() );
        assertThat( await( tx.commitAsync() ), is( nullValue() ) );

        String bookmarkAfter = session.lastBookmark();

        assertNotNull( bookmarkAfter );
        assertNotEquals( bookmarkBefore, bookmarkAfter );
    }

    @Test
    void shouldBePossibleToRollbackEmptyTx()
    {
        String bookmarkBefore = session.lastBookmark();

        AsyncTransaction tx = await( session.beginTransactionAsync() );
        assertThat( await( tx.rollbackAsync() ), is( nullValue() ) );

        String bookmarkAfter = session.lastBookmark();
        assertEquals( bookmarkBefore, bookmarkAfter );
    }

    @Test
    void shouldBePossibleToRunSingleStatementAndCommit()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor = await( tx.runAsync( "CREATE (n:Node {id: 42}) RETURN n" ) );

        Record record = await( cursor.nextAsync() );
        assertNotNull( record );
        Node node = record.get( 0 ).asNode();
        assertEquals( "Node", single( node.labels() ) );
        assertEquals( 42, node.get( "id" ).asInt() );
        assertNull( await( cursor.nextAsync() ) );

        assertNull( await( tx.commitAsync() ) );
        assertEquals( 1, countNodes( 42 ) );
    }

    @Test
    void shouldBePossibleToRunSingleStatementAndRollback()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor = await( tx.runAsync( "CREATE (n:Node {id: 4242}) RETURN n" ) );
        Record record = await( cursor.nextAsync() );
        assertNotNull( record );
        Node node = record.get( 0 ).asNode();
        assertEquals( "Node", single( node.labels() ) );
        assertEquals( 4242, node.get( "id" ).asInt() );
        assertNull( await( cursor.nextAsync() ) );

        assertNull( await( tx.rollbackAsync() ) );
        assertEquals( 0, countNodes( 4242 ) );
    }

    @Test
    void shouldBePossibleToRunMultipleStatementsAndCommit()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor1 = await( tx.runAsync( "CREATE (n:Node {id: 1})" ) );
        assertNull( await( cursor1.nextAsync() ) );

        StatementResultCursor cursor2 = await( tx.runAsync( "CREATE (n:Node {id: 2})" ) );
        assertNull( await( cursor2.nextAsync() ) );

        StatementResultCursor cursor3 = await( tx.runAsync( "CREATE (n:Node {id: 2})" ) );
        assertNull( await( cursor3.nextAsync() ) );

        assertNull( await( tx.commitAsync() ) );
        assertEquals( 1, countNodes( 1 ) );
        assertEquals( 2, countNodes( 2 ) );
    }

    @Test
    void shouldBePossibleToRunMultipleStatementsAndCommitWithoutWaiting()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        tx.runAsync( "CREATE (n:Node {id: 1})" );
        tx.runAsync( "CREATE (n:Node {id: 2})" );
        tx.runAsync( "CREATE (n:Node {id: 1})" );

        assertNull( await( tx.commitAsync() ) );
        assertEquals( 1, countNodes( 2 ) );
        assertEquals( 2, countNodes( 1 ) );
    }

    @Test
    void shouldBePossibleToRunMultipleStatementsAndRollback()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor1 = await( tx.runAsync( "CREATE (n:Node {id: 1})" ) );
        assertNull( await( cursor1.nextAsync() ) );

        StatementResultCursor cursor2 = await( tx.runAsync( "CREATE (n:Node {id: 42})" ) );
        assertNull( await( cursor2.nextAsync() ) );

        assertNull( await( tx.rollbackAsync() ) );
        assertEquals( 0, countNodes( 1 ) );
        assertEquals( 0, countNodes( 42 ) );
    }

    @Test
    void shouldBePossibleToRunMultipleStatementsAndRollbackWithoutWaiting()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        tx.runAsync( "CREATE (n:Node {id: 1})" );
        tx.runAsync( "CREATE (n:Node {id: 42})" );

        assertNull( await( tx.rollbackAsync() ) );
        assertEquals( 0, countNodes( 1 ) );
        assertEquals( 0, countNodes( 42 ) );
    }

    @Test
    void shouldFailToCommitAfterSingleWrongStatement()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor = await( tx.runAsync( "RETURN" ) );

        Exception e = assertThrows( Exception.class, () -> await( cursor.consumeAsync() ) );
        assertThat( e, is( syntaxError( "Unexpected end of input" ) ) );

        assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
    }

    @Test
    void shouldAllowRollbackAfterSingleWrongStatement()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor = await( tx.runAsync( "RETURN" ) );

        Exception e = assertThrows( Exception.class, () -> await( cursor.nextAsync() ) );
        assertThat( e, is( syntaxError( "Unexpected end of input" ) ) );
        assertThat( await( tx.rollbackAsync() ), is( nullValue() ) );
    }

    @Test
    void shouldFailToCommitAfterCoupleCorrectAndSingleWrongStatement()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor1 = await( tx.runAsync( "CREATE (n:Node) RETURN n" ) );
        Record record1 = await( cursor1.nextAsync() );
        assertNotNull( record1 );
        assertTrue( record1.get( 0 ).asNode().hasLabel( "Node" ) );

        StatementResultCursor cursor2 = await( tx.runAsync( "RETURN 42" ) );
        Record record2 = await( cursor2.nextAsync() );
        assertNotNull( record2 );
        assertEquals( 42, record2.get( 0 ).asInt() );

        StatementResultCursor cursor3 = await( tx.runAsync( "RETURN" ) );

        Exception e = assertThrows( Exception.class, () -> await( cursor3.consumeAsync() ) );
        assertThat( e, is( syntaxError( "Unexpected end of input" ) ) );

        assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
    }

    @Test
    void shouldAllowRollbackAfterCoupleCorrectAndSingleWrongStatement()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor1 = await( tx.runAsync( "RETURN 4242" ) );
        Record record1 = await( cursor1.nextAsync() );
        assertNotNull( record1 );
        assertEquals( 4242, record1.get( 0 ).asInt() );

        StatementResultCursor cursor2 = await( tx.runAsync( "CREATE (n:Node) DELETE n RETURN 42" ) );
        Record record2 = await( cursor2.nextAsync() );
        assertNotNull( record2 );
        assertEquals( 42, record2.get( 0 ).asInt() );

        StatementResultCursor cursor3 = await( tx.runAsync( "RETURN" ) );

        Exception e = assertThrows( Exception.class, () -> await( cursor3.summaryAsync() ) );
        assertThat( e, is( syntaxError( "Unexpected end of input" ) ) );
        assertThat( await( tx.rollbackAsync() ), is( nullValue() ) );
    }

    @Test
    void shouldNotAllowNewStatementsAfterAnIncorrectStatement()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor = await( tx.runAsync( "RETURN" ) );

        Exception e1 = assertThrows( Exception.class, () -> await( cursor.nextAsync() ) );
        assertThat( e1, is( syntaxError( "Unexpected end of input" ) ) );

        ClientException e2 = assertThrows( ClientException.class, () -> tx.runAsync( "CREATE ()" ) );
        assertThat( e2.getMessage(), startsWith( "Cannot run more statements in this transaction" ) );
    }

    @Test
    void shouldFailBoBeginTxWithInvalidBookmark()
    {
        AsyncSession session = neo4j.driver().asyncSession( t -> t.withBookmarks( "InvalidBookmark" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( session.beginTransactionAsync() ) );
        assertThat( e.getMessage(), containsString( "InvalidBookmark" ) );
    }

    @Test
    void shouldBePossibleToCommitWhenCommitted()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        tx.runAsync( "CREATE ()" );
        assertNull( await( tx.commitAsync() ) );

        CompletionStage<Void> secondCommit = tx.commitAsync();
        // second commit should return a completed future
        assertTrue( secondCommit.toCompletableFuture().isDone() );
        assertNull( await( secondCommit ) );
    }

    @Test
    void shouldBePossibleToRollbackWhenRolledBack()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        tx.runAsync( "CREATE ()" );
        assertNull( await( tx.rollbackAsync() ) );

        CompletionStage<Void> secondRollback = tx.rollbackAsync();
        // second rollback should return a completed future
        assertTrue( secondRollback.toCompletableFuture().isDone() );
        assertNull( await( secondRollback ) );
    }

    @Test
    void shouldFailToCommitWhenRolledBack()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        tx.runAsync( "CREATE ()" );
        assertNull( await( tx.rollbackAsync() ) );

        // should not be possible to commit after rollback
        ClientException e = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertThat( e.getMessage(), containsString( "transaction has been rolled back" ) );
    }

    @Test
    void shouldFailToRollbackWhenCommitted()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        tx.runAsync( "CREATE ()" );
        assertNull( await( tx.commitAsync() ) );

        // should not be possible to rollback after commit
        ClientException e = assertThrows( ClientException.class, () -> await( tx.rollbackAsync() ) );
        assertThat( e.getMessage(), containsString( "transaction has been committed" ) );
    }

    @Test
    void shouldExposeStatementKeysForColumnsWithAliases()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "RETURN 1 AS one, 2 AS two, 3 AS three, 4 AS five" ) );

        assertEquals( Arrays.asList( "one", "two", "three", "five" ), cursor.keys() );
    }

    @Test
    void shouldExposeStatementKeysForColumnsWithoutAliases()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "RETURN 1, 2, 3, 5" ) );

        assertEquals( Arrays.asList( "1", "2", "3", "5" ), cursor.keys() );
    }

    @Test
    void shouldExposeResultSummaryForSimpleQuery()
    {
        String query = "CREATE (p1:Person {name: $name1})-[:KNOWS]->(p2:Person {name: $name2}) RETURN p1, p2";
        Value params = parameters( "name1", "Bob", "name2", "John" );

        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( query, params ) );
        ResultSummary summary = await( cursor.summaryAsync() );

        assertEquals( new Statement( query, params ), summary.statement() );
        assertEquals( 2, summary.counters().nodesCreated() );
        assertEquals( 2, summary.counters().labelsAdded() );
        assertEquals( 2, summary.counters().propertiesSet() );
        assertEquals( 1, summary.counters().relationshipsCreated() );
        assertEquals( StatementType.READ_WRITE, summary.statementType() );
        assertFalse( summary.hasPlan() );
        assertFalse( summary.hasProfile() );
        assertNull( summary.plan() );
        assertNull( summary.profile() );
        assertEquals( 0, summary.notifications().size() );
        assertThat( summary, containsResultAvailableAfterAndResultConsumedAfter() );
    }

    @Test
    void shouldExposeResultSummaryForExplainQuery()
    {
        String query = "EXPLAIN MATCH (n) RETURN n";

        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( query ) );
        ResultSummary summary = await( cursor.summaryAsync() );

        assertEquals( new Statement( query ), summary.statement() );
        assertEquals( 0, summary.counters().nodesCreated() );
        assertEquals( 0, summary.counters().propertiesSet() );
        assertEquals( StatementType.READ_ONLY, summary.statementType() );
        assertTrue( summary.hasPlan() );
        assertFalse( summary.hasProfile() );
        assertNotNull( summary.plan() );
        // asserting on plan is a bit fragile and can break when server side changes or with different
        // server versions; that is why do fuzzy assertions in this test based on string content
        assertThat( summary.plan().toString().toLowerCase(), containsString( "scan" ) );
        assertNull( summary.profile() );
        assertEquals( 0, summary.notifications().size() );
        assertThat( summary, containsResultAvailableAfterAndResultConsumedAfter() );
    }

    @Test
    void shouldExposeResultSummaryForProfileQuery()
    {
        String query = "PROFILE MERGE (n {name: $name}) " +
                       "ON CREATE SET n.created = timestamp() " +
                       "ON MATCH SET n.counter = coalesce(n.counter, 0) + 1";

        Value params = parameters( "name", "Bob" );

        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( query, params ) );
        ResultSummary summary = await( cursor.summaryAsync() );

        assertEquals( new Statement( query, params ), summary.statement() );
        assertEquals( 1, summary.counters().nodesCreated() );
        assertEquals( 2, summary.counters().propertiesSet() );
        assertEquals( 0, summary.counters().relationshipsCreated() );
        assertEquals( StatementType.WRITE_ONLY, summary.statementType() );
        assertTrue( summary.hasPlan() );
        assertTrue( summary.hasProfile() );
        assertNotNull( summary.plan() );
        assertNotNull( summary.profile() );
        // asserting on profile is a bit fragile and can break when server side changes or with different
        // server versions; that is why do fuzzy assertions in this test based on string content
        String profileAsString = summary.profile().toString().toLowerCase();
        assertThat( profileAsString, containsString( "hits" ) );
        assertEquals( 0, summary.notifications().size() );
        assertThat( summary, containsResultAvailableAfterAndResultConsumedAfter() );
    }

    @Test
    void shouldPeekRecordFromCursor()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "UNWIND ['a', 'b', 'c'] AS x RETURN x" ) );

        assertEquals( "a", await( cursor.peekAsync() ).get( 0 ).asString() );
        assertEquals( "a", await( cursor.peekAsync() ).get( 0 ).asString() );

        assertEquals( "a", await( cursor.nextAsync() ).get( 0 ).asString() );

        assertEquals( "b", await( cursor.peekAsync() ).get( 0 ).asString() );
        assertEquals( "b", await( cursor.peekAsync() ).get( 0 ).asString() );
        assertEquals( "b", await( cursor.peekAsync() ).get( 0 ).asString() );

        assertEquals( "b", await( cursor.nextAsync() ).get( 0 ).asString() );
        assertEquals( "c", await( cursor.nextAsync() ).get( 0 ).asString() );

        assertNull( await( cursor.peekAsync() ) );
        assertNull( await( cursor.nextAsync() ) );

        await( tx.rollbackAsync() );
    }

    @Test
    void shouldForEachWithEmptyCursor()
    {
        testForEach( "MATCH (n:SomeReallyStrangeLabel) RETURN n", 0 );
    }

    @Test
    void shouldForEachWithNonEmptyCursor()
    {
        testForEach( "UNWIND range(1, 12555) AS x CREATE (n:Node {id: x}) RETURN n", 12555 );
    }

    @Test
    void shouldFailForEachWhenActionFails()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "RETURN 'Hi!'" ) );
        RuntimeException error = new RuntimeException();

        RuntimeException e = assertThrows( RuntimeException.class, () ->
        {
            await( cursor.forEachAsync( record ->
            {
                throw error;
            } ) );
        } );
        assertEquals( error, e );
    }

    @Test
    void shouldConvertToListWithEmptyCursor()
    {
        testList( "CREATE (:Person)-[:KNOWS]->(:Person)", Collections.emptyList() );
    }

    @Test
    void shouldConvertToListWithNonEmptyCursor()
    {
        testList( "UNWIND [1, '1', 2, '2', 3, '3'] AS x RETURN x", Arrays.asList( 1L, "1", 2L, "2", 3L, "3" ) );
    }

    @Test
    void shouldConvertToTransformedListWithEmptyCursor()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "CREATE ()" ) );
        List<Map<String,Object>> maps = await( cursor.listAsync( record -> record.get( 0 ).asMap() ) );
        assertEquals( 0, maps.size() );
    }

    @Test
    void shouldConvertToTransformedListWithNonEmptyCursor()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "UNWIND ['a', 'b', 'c'] AS x RETURN x" ) );
        List<String> strings = await( cursor.listAsync( record -> record.get( 0 ).asString() + "!" ) );
        assertEquals( Arrays.asList( "a!", "b!", "c!" ), strings );
    }

    @Test
    void shouldFailWhenListTransformationFunctionFails()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "RETURN 'Hello'" ) );
        IOException error = new IOException( "World" );

        Exception e = assertThrows( Exception.class, () ->
                await( cursor.listAsync( record ->
                {
                    throw new CompletionException( error );
                } ) ) );
        assertEquals( error, e );
    }

    @Test
    void shouldFailToCommitWhenServerIsRestarted()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        await( tx.runAsync( "CREATE ()" ) );

        neo4j.stopDb();

        assertThrows( ServiceUnavailableException.class, () -> await( tx.commitAsync() ) );
    }

    @Test
    void shouldFailSingleWithEmptyCursor()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "MATCH (n:NoSuchLabel) RETURN n" ) );

        NoSuchRecordException e = assertThrows( NoSuchRecordException.class, () -> await( cursor.singleAsync() ) );
        assertThat( e.getMessage(), containsString( "result is empty" ) );
    }

    @Test
    void shouldFailSingleWithMultiRecordCursor()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "UNWIND ['a', 'b'] AS x RETURN x" ) );

        NoSuchRecordException e = assertThrows( NoSuchRecordException.class, () -> await( cursor.singleAsync() ) );
        assertThat( e.getMessage(), startsWith( "Expected a result with a single record" ) );
    }

    @Test
    void shouldReturnSingleWithSingleRecordCursor()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "RETURN 'Hello!'" ) );

        Record record = await( cursor.singleAsync() );

        assertEquals( "Hello!", record.get( 0 ).asString() );
    }

    @Test
    void shouldPropagateFailureFromFirstRecordInSingleAsync()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "UNWIND [0] AS x RETURN 10 / x" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( cursor.singleAsync() ) );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
    }

    @Test
    void shouldNotPropagateFailureFromSecondRecordInSingleAsync()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "UNWIND [1, 0] AS x RETURN 10 / x" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( cursor.singleAsync() ) );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
    }

    @Test
    void shouldConsumeEmptyCursor()
    {
        testConsume( "MATCH (n:NoSuchLabel) RETURN n" );
    }

    @Test
    void shouldConsumeNonEmptyCursor()
    {
        testConsume( "RETURN 42" );
    }

    @Test
    void shouldFailToRunQueryAfterCommit()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        tx.runAsync( "CREATE (:MyLabel)" );
        assertNull( await( tx.commitAsync() ) );

        StatementResultCursor cursor = await( session.runAsync( "MATCH (n:MyLabel) RETURN count(n)" ) );
        assertEquals( 1, await( cursor.singleAsync() ).get( 0 ).asInt() );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.runAsync( "CREATE (:MyOtherLabel)" ) ) );
        assertEquals( "Cannot run more statements in this transaction, it has been committed", e.getMessage() );
    }

    @Test
    void shouldFailToRunQueryAfterRollback()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        tx.runAsync( "CREATE (:MyLabel)" );
        assertNull( await( tx.rollbackAsync() ) );

        StatementResultCursor cursor = await( session.runAsync( "MATCH (n:MyLabel) RETURN count(n)" ) );
        assertEquals( 0, await( cursor.singleAsync() ).get( 0 ).asInt() );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.runAsync( "CREATE (:MyOtherLabel)" ) ) );
        assertEquals( "Cannot run more statements in this transaction, it has been rolled back", e.getMessage() );
    }

    @Test
    void shouldUpdateSessionBookmarkAfterCommit()
    {
        String bookmarkBefore = session.lastBookmark();

        await( session.beginTransactionAsync()
                .thenCompose( tx -> tx.runAsync( "CREATE (:MyNode)" )
                        .thenCompose( ignore -> tx.commitAsync() ) ) );

        String bookmarkAfter = session.lastBookmark();

        assertNotNull( bookmarkAfter );
        assertNotEquals( bookmarkBefore, bookmarkAfter );
    }

    @Test
    void shouldFailToCommitWhenQueriesFailAndErrorNotConsumed() throws InterruptedException
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        tx.runAsync( "CREATE (:TestNode)" );
        tx.runAsync( "CREATE (:TestNode)" );
        tx.runAsync( "RETURN 10 / 0" );
        tx.runAsync( "CREATE (:TestNode)" );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertEquals( "/ by zero", e.getMessage() );
    }

    @Test
    void shouldPropagateRunFailureFromCommit()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        tx.runAsync( "RETURN ILLEGAL" );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertThat( e.getMessage(), containsString( "ILLEGAL" ) );
    }

    @Test
    void shouldPropagateBlockedRunFailureFromCommit()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        await( tx.runAsync( "RETURN 42 / 0" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
    }

    @Test
    void shouldPropagateRunFailureFromRollback()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        tx.runAsync( "RETURN ILLEGAL" );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.rollbackAsync() ) );
        assertThat( e.getMessage(), containsString( "ILLEGAL" ) );
    }

    @Test
    void shouldPropagateBlockedRunFailureFromRollback()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        await( tx.runAsync( "RETURN 42 / 0" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.rollbackAsync() ) );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
    }

    @Test
    void shouldPropagatePullAllFailureFromCommit()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        tx.runAsync( "UNWIND [1, 2, 3, 'Hi'] AS x RETURN 10 / x" );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertThat( e.code(), containsString( "TypeError" ) );
    }

    @Test
    void shouldPropagateBlockedPullAllFailureFromCommit()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        await( tx.runAsync( "UNWIND [1, 2, 3, 'Hi'] AS x RETURN 10 / x" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertThat( e.code(), containsString( "TypeError" ) );
    }

    @Test
    void shouldPropagatePullAllFailureFromRollback()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        tx.runAsync( "UNWIND [1, 2, 3, 'Hi'] AS x RETURN 10 / x" );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.rollbackAsync() ) );
        assertThat( e.code(), containsString( "TypeError" ) );
    }

    @Test
    void shouldPropagateBlockedPullAllFailureFromRollback()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        await( tx.runAsync( "UNWIND [1, 2, 3, 'Hi'] AS x RETURN 10 / x" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.rollbackAsync() ) );
        assertThat( e.code(), containsString( "TypeError" ) );
    }

    @Test
    void shouldFailToCommitWhenRunFailureIsConsumed()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "RETURN Wrong" ) );

        ClientException e1 = assertThrows( ClientException.class, () -> await( cursor.consumeAsync() ) );
        assertThat( e1.code(), containsString( "SyntaxError" ) );

        ClientException e2 = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertThat( e2.getMessage(), startsWith( "Transaction can't be committed" ) );
    }

    @Test
    void shouldFailToCommitWhenPullAllFailureIsConsumed()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync(
                "FOREACH (value IN [1,2, 'aaa'] | CREATE (:Person {name: 10 / value}))" ) );

        ClientException e1 = assertThrows( ClientException.class, () -> await( cursor.consumeAsync() ) );
        assertThat( e1.code(), containsString( "TypeError" ) );

        ClientException e2 = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertThat( e2.getMessage(), startsWith( "Transaction can't be committed" ) );
    }

    @Test
    void shouldRollbackWhenRunFailureIsConsumed()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "RETURN Wrong" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( cursor.consumeAsync() ) );
        assertThat( e.code(), containsString( "SyntaxError" ) );
        assertNull( await( tx.rollbackAsync() ) );
    }

    @Test
    void shouldRollbackWhenPullAllFailureIsConsumed()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "UNWIND [1, 0] AS x RETURN 5 / x" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( cursor.consumeAsync() ) );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
        assertNull( await( tx.rollbackAsync() ) );
    }

    @Test
    void shouldPropagateFailureFromSummary()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor = await( tx.runAsync( "RETURN Wrong" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( cursor.summaryAsync() ) );
        assertThat( e.code(), containsString( "SyntaxError" ) );
        assertNotNull( await( cursor.summaryAsync() ) );
    }

    private int countNodes( Object id )
    {
        StatementResultCursor cursor = await( session.runAsync( "MATCH (n:Node {id: $id}) RETURN count(n)", parameters( "id", id ) ) );
        return await( cursor.singleAsync() ).get( 0 ).asInt();
    }

    private void testForEach( String query, int expectedSeenRecords )
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( query ) );

        AtomicInteger recordsSeen = new AtomicInteger();
        CompletionStage<ResultSummary> forEachDone = cursor.forEachAsync( record -> recordsSeen.incrementAndGet() );
        ResultSummary summary = await( forEachDone );

        assertNotNull( summary );
        assertEquals( query, summary.statement().text() );
        assertEquals( emptyMap(), summary.statement().parameters().asMap() );
        assertEquals( expectedSeenRecords, recordsSeen.get() );
    }

    private <T> void testList( String query, List<T> expectedList )
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( query ) );
        List<Record> records = await( cursor.listAsync() );
        List<Object> actualList = new ArrayList<>();
        for ( Record record : records )
        {
            actualList.add( record.get( 0 ).asObject() );
        }
        assertEquals( expectedList, actualList );
    }

    private void testConsume( String query )
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( query ) );
        ResultSummary summary = await( cursor.consumeAsync() );

        assertNotNull( summary );
        assertEquals( query, summary.statement().text() );
        assertEquals( emptyMap(), summary.statement().parameters().asMap() );

        // no records should be available, they should all be consumed
        assertNull( await( cursor.nextAsync() ) );
    }
}
