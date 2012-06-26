/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.huahinframework.unit;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.huahinframework.core.Summarizer;
import org.huahinframework.core.io.Key;
import org.huahinframework.core.io.Record;
import org.huahinframework.core.io.Value;
import org.junit.Before;

/**
 * This is a test driver of a {@link Summarizer} class.
 *
 * <p>Example:</p>
 * <p><blockquote><pre>
 * public class SummarizerTest extends SummarizerDriver {
 *   public void test() {
 *     Record input = new Record();
 *     input.addValue("label", "label");
 *     input.addValue("value", 1);
 *
 *     Record output = new Record();
 *     output.addGrouping("label", "label");
 *     output.addValue("value", 1);
 *
 *     run(Arrays.asList(input), Arrays.asList(output));
 *   }
 *
 *   public Summarizer getSummarizer() {
 *     return new TestSummarizer();
 *   }
 * }
 * </pre></blockquote></p>
 */
public abstract class SummarizerDriver {
    private Reducer<Key, Value, Key, Value> reducer;
    private ReduceDriver<Key, Value, Key, Value> driver;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        reducer = getSummarizer();
        driver = new ReduceDriver<Key, Value, Key, Value>(reducer);
    }

    /**
     * Run the test with this method.
     * @param input input {@link Record} {@link List}
     * @param output result of {@link Record} {@link List}
     */
    public void run(List<Record> input, List<Record> output) {
        if (input.size() < 0) {
            fail("input size is 0");
        }

        Key key = input.get(0).getKey();
        List<Value> values = new ArrayList<Value>();
        for (Record record : input) {
            values.add(record.getValue());
        }

        driver.withInput(key, values);

        if (output != null) {
            for (Record r : output) {
                driver.withOutput(r.getKey(), r.getValue());
            }
        }

        driver.runTest();
    }

    /**
     * Set the {@link Summarizer} class in this method.
     * @return new {@link Summarizer}
     */
    public abstract Summarizer getSummarizer();
}
