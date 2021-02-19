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
package org.neo4j.driver.internal.metrics;

import org.neo4j.driver.internal.metrics.spi.Histogram;

public class HistogramSnapshot implements Histogram
{
    private final Histogram copy;
    private final Histogram origin;

    public HistogramSnapshot( Histogram copy, Histogram origin )
    {
        this.copy = copy;
        this.origin = origin;
    }

    @Override
    public long min()
    {
        return copy.min();
    }

    @Override
    public long max()
    {
        return copy.max();
    }

    @Override
    public double mean()
    {
        return copy.mean();
    }

    @Override
    public double stdDeviation()
    {
        return copy.stdDeviation();
    }

    @Override
    public long totalCount()
    {
        return copy.totalCount();
    }

    @Override
    public long valueAtPercentile( double percentile )
    {
        return copy.valueAtPercentile( percentile );
    }

    @Override
    public void reset()
    {
        origin.reset();
    }

    @Override
    public String toString()
    {
        return copy.toString();
    }
}
