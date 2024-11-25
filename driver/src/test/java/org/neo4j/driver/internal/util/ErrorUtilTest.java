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
package org.neo4j.driver.internal.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.util.ErrorUtil.newConnectionTerminatedError;
import static org.neo4j.driver.internal.util.ErrorUtil.rethrowAsyncException;

import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.exceptions.Neo4jException;

class ErrorUtilTest {

    @Test
    void shouldCreateConnectionTerminatedError() {
        var error = newConnectionTerminatedError();
        assertThat(error.getMessage(), startsWith("Connection to the database terminated"));
    }

    @Test
    void shouldCreateConnectionTerminatedErrorWithNullReason() {
        var error = newConnectionTerminatedError(null);
        assertThat(error.getMessage(), startsWith("Connection to the database terminated"));
    }

    @Test
    void shouldCreateConnectionTerminatedErrorWithReason() {
        var reason = "Thread interrupted";
        var error = newConnectionTerminatedError(reason);
        assertThat(error.getMessage(), startsWith("Connection to the database terminated"));
        assertThat(error.getMessage(), containsString(reason));
    }

    @Test
    void shouldWrapCheckedExceptionsInNeo4jExceptionWhenRethrowingAsyncException() {
        var ee = mock(ExecutionException.class);
        var uhe = mock(UnknownHostException.class);
        given(ee.getCause()).willReturn(uhe);
        given(uhe.getStackTrace()).willReturn(new StackTraceElement[0]);

        var actual = assertThrows(Neo4jException.class, () -> rethrowAsyncException(ee));

        assertEquals(actual.getCause(), uhe);
    }
}
