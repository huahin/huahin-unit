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
package org.huahin.unit;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.huahin.core.DataFormatException;
import org.huahin.core.Filter;
import org.huahin.core.SimpleJob;
import org.huahin.core.io.Key;
import org.huahin.core.io.Record;
import org.huahin.core.io.Value;
import org.junit.Before;

/**
 * This is a test driver of a {@link Filter} class.
 *
 * <p>Example:</p>
 * <p><blockquote><pre>
 * public class FilterTest extends FilterDriver {
 *   private static final String LABELS = new String[] { "label", "value" };
 *
 *   public void testString() {
 *     String input = "label\t1";
 *
 *     Record output = new Record();
 *     output.addGrouping("label", "label");
 *     output.addValue("value", 1);
 *
 *     run(LABELS, "\t", false, input, Arrays.asList(output));
 *   }
 *
 *   public void testRecord() {
 *     Record input = new Record();
 *     input.addValue("label", "label");
 *     input.addValue("value", 1);
 *
 *     Record output = new Record();
 *     output.addGrouping("label", "label");
 *     output.addValue("value", 1);
 *
 *     run(input, Arrays.asList(output));
 *   }
 *
 *   public Filter getFilter() {
 *     return new TestFilter();
 *   }
 * }
 * </pre></blockquote></p>
 */
public abstract class FilterDriver {
    private Mapper<Writable, Writable, Key, Value> mapper;
    private MapDriver<Writable, Writable, Key, Value> driver;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        mapper = getFilter();
        driver = new MapDriver<Writable, Writable, Key, Value>(mapper);
    }

    /**
     * Run the test with this method.
     * The data is input for the {@link Record}.
     * @param input input {@link Record}
     * @param output result of {@link Record} {@link List}
     */
    public void run(Record input, List<Record> output) {
        driver.withInput(input.getKey(), input.getValue());

        if (output != null) {
            for (Record r : output) {
                driver.withOutput(r.getKey(), r.getValue());
            }
        }

        driver.runTest();
    }

    /**
     * Run the test with this method.
     * The data is input for the {@link String}.
     * @param labels label of input data
     * @param separator separator of data
     * @param formatIgnored
     * If true, {@link DataFormatException} will be throw if there is a format error.
     * If false is ignored (default).
     * @param input input {@link String} data
     * @param output result of {@link Record} {@link List}
     */
    public void run(String[] labels,
                    String separator,
                    boolean formatIgnored,
                    String input,
                    List<Record> output) {
        Configuration conf = new Configuration();
        conf.setStrings(SimpleJob.LABELS, labels);
        conf.set(SimpleJob.SEPARATOR, separator);
        conf.setBoolean(SimpleJob.FIRST, true);
        conf.setBoolean(SimpleJob.FORMAT_IGNORED, formatIgnored);
        driver.setConfiguration(conf);

        driver.withInput(new LongWritable(1L), new Text(input));

        if (output != null) {
            for (Record r : output) {
                driver.withOutput(r.getKey(), r.getValue());
            }
        }

        driver.runTest();
    }

    /**
     * Set the {@link Filter} class in this method.
     * @return new {@link Filter}
     */
    public abstract Filter getFilter();
}
