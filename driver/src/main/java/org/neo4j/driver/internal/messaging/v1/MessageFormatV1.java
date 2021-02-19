/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.driver.internal.messaging.v1;

import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.internal.packstream.PackOutput;

public class MessageFormatV1 implements MessageFormat
{
    public static final byte NODE = 'N';
    public static final byte RELATIONSHIP = 'R';
    public static final byte UNBOUND_RELATIONSHIP = 'r';
    public static final byte PATH = 'P';

    public static final int NODE_FIELDS = 3;

    @Override
    public MessageFormat.Writer newWriter( PackOutput output )
    {
        return new MessageWriterV1( output );
    }

    @Override
    public MessageFormat.Reader newReader( PackInput input )
    {
        return new MessageReaderV1( input );
    }
}
