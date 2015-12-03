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
package org.neo4j.driver.v1.internal.value;

import org.junit.Test;

import java.util.HashMap;

import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.internal.SimpleNode;
import org.neo4j.driver.v1.internal.types.StandardTypeSystem;
import org.neo4j.driver.v1.internal.types.TypeConstructor;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.v1.Values.value;

public class ListValueTest
{
    @Test
    public void shouldHaveSensibleToString() throws Throwable
    {
        ListValue listValue = listValue( value( 1 ), value( 2 ), value( 3 ) );
        assertThat( listValue.toString(), equalTo( "ListValue[integer<1>, integer<2>, integer<3>]" ) );
    }

    @Test
    public void shouldHaveCorrectType() throws Throwable
    {

        ListValue listValue = listValue();

        assertThat(listValue.type(), equalTo( StandardTypeSystem.TYPE_SYSTEM.LIST() ));
    }

    @Test
    public void testConversionsFromListValue() throws Throwable
    {
        ListValue listValue = listValue( value( 1 ), value( 2 ), value( 3 ) );

        assertThat( listValue.asArray(), equalTo( new Value[]{value( 1 ), value( 2 ), value( 3 )} ) );
        assertThat( listValue.asByteArray(), equalTo( new byte[]{1, 2, 3} ) );
        assertThat( listValue.asDoubleArray(), equalTo( new double[]{1D, 2D, 3D} ) );
        assertThat( listValue.asFloatArray(), equalTo( new float[]{1F, 2F, 3F} ) );
        assertThat( listValue.asIntArray(), equalTo( new int[]{1, 2, 3} ) );
        assertThat( listValue.asLongArray(), equalTo( new long[]{1L, 2L, 3L} ) );
        assertThat( listValue.asShortArray(), equalTo( new short[]{1, 2, 3} ) );
    }


    private ListValue listValue( Value... values )
    {
        return new ListValue( values );
    }
}
