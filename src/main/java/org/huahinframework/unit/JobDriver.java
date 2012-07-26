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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.huahinframework.core.DataFormatException;
import org.huahinframework.core.SimpleJob;
import org.huahinframework.core.SimpleJobTool;
import org.huahinframework.core.io.Key;
import org.huahinframework.core.io.Record;
import org.huahinframework.core.io.Value;
import org.huahinframework.core.util.StringUtil;
import org.junit.Before;

/**
 * This is a test driver of a Job.
 *
 * <p>Example:</p>
 * <p><blockquote><pre>
 * public class JobTest extends JobDriver {
 *   private static final String LABELS = new String[] { "label", "value" };
 *   public void test()
 *       throws IOException, InstantiationException,
 *              IllegalAccessException, ClassNotFoundException {
 *     addJob(LABELS, StringUtil.TAB, false).setFilter(TestFilter.class)
 *                                          .setSummaizer(TestSummarizer.class);
 *
 *     List<String> input = new ArrayList<String>();
 *     input.add("label\t1");
 *     input.add("label\t2");
 *     input.add("label\t3");
 *
 *     List<Record> output = new ArrayList<Record>();
 *     Record record = new Record();
 *     record.addGrouping("label", "label");
 *     record.addValue("value", 6);
 *     output.add(record);
 *
 *     run(input, output, true);
 *   }
 * }
 * </pre></blockquote></p>
 */
public abstract class JobDriver extends SimpleJobTool {
    private static final String OUTPUT = "(%s, %s)";

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        jobName = StringUtil.createInternalJobID();
    }

    /**
     * Run the test with this method.
     * @param input input {@link String} {@link List} or {@link Record} {@link List}
     * @param output result of {@link Record} {@link List}
     * @param stdout whether to output the execution results to stdout.
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public void run(List<?> input, List<Record> output, boolean stdout)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {
        if (input == null || input.isEmpty()) {
            fail("input is empty");
        }

        List<Pair<Key, Value>> actual = null;
        Object o = input.get(0);
        if (o instanceof String) {
            actual = runString((List<String>) input);
        } else if (o instanceof Record) {
            actual = runRecord((List<Record>) input);
        } else {
            fail("input object is miss match");
        }

        if (stdout) {
            for (Pair<Key, Value> pair : actual) {
                System.out.println(pair);
            }
        }

        assertEquals(output.size(), actual.size());

        if (output != null) {
            for (int i = 0; i < output.size(); i++) {
                Pair<Key, Value> pair = actual.get(i);
                Record r = output.get(i);
                String s = String.format(OUTPUT, r.getKey().toString(),
                                                 r.getValue().toString());
                assertEquals(s, pair.toString());
            }
        }
    }

    /**
     * @param input
     * @return List<Pair<Key, Value>>
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    private List<Pair<Key, Value>> runString(List<? extends String> input)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {
        boolean first = true;
        List<Pair<Key, Value>> actual = null;

        for (Job job : sequencalJobChain.getJobs()) {
            MapReduceDriver<Key, Value, Key, Value, Key, Value> driver = createDriver(job, false);

            if (first) {
                boolean formatIgnored =
                        sequencalJobChain.getJobs().get(0).getConfiguration().getBoolean(SimpleJob.FORMAT_IGNORED, false);
                String[] labels =
                        sequencalJobChain.getJobs().get(0).getConfiguration().getStrings(SimpleJob.LABELS);
                String separator =
                        sequencalJobChain.getJobs().get(0).getConfiguration().get(SimpleJob.SEPARATOR, StringUtil.COMMA);

                for (String s : input) {
                    Key key = new Key();
                    key.addPrimitiveValue("KEY", 1L);
                    Value value = new Value();
                    String[] strings = StringUtil.split(s, separator, false);
                    if (labels != null) {
                        if (labels.length != strings.length) {
                            if (formatIgnored) {
                                throw new DataFormatException("input format error: " +
                                                              "label.length = " + labels.length +
                                                              "input.lenght = " + strings.length);
                            }
                        }

                        for (int i = 0; i < strings.length; i++) {
                            value.addPrimitiveValue(labels[i], strings[i]);
                        }
                    } else {
                        for (int i = 0; i < strings.length; i++) {
                            value.addPrimitiveValue(String.valueOf(i), strings[i]);
                        }
                    }

                    driver.addInput(key, value);
                }
                first = false;
            } else {
                for (Pair<Key, Value> pair : actual) {
                    driver.addInput(pair.getFirst(), pair.getSecond());
                }
            }

            actual = driver.run();
        }

        return actual;
    }

    /**
     * @param input
     * @return List<Pair<Key, Value>>
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    private List<Pair<Key, Value>> runRecord(List<? extends Record> input)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {
        boolean first = true;
        List<Pair<Key, Value>> actual = null;

        for (Job job : sequencalJobChain.getJobs()) {
            MapReduceDriver<Key, Value, Key, Value, Key, Value> driver = createDriver(job, true);

            if (first) {
                for (Record r : input) {
                    driver.addInput(r.getKey(), r.getValue());
                }
                first = false;
            } else {
                for (Pair<Key, Value> pair : actual) {
                    driver.addInput(pair.getFirst(), pair.getSecond());
                }
            }

            actual = driver.run();
        }

        return actual;
    }

    /**
     * @param job
     * @param record
     * @return List<Pair<Key, Value>>
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private MapReduceDriver<Key, Value, Key, Value, Key, Value> createDriver(Job job, boolean record)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        Mapper mapper = job.getMapperClass().newInstance();
        Reducer reducer = job.getReducerClass().newInstance();
        RawComparator groupingComparator = job.getGroupingComparator();
        RawComparator sortComparator = job.getSortComparator();
        MapReduceDriver<Key, Value, Key, Value, Key, Value> driver =
                MapReduceDriver.newMapReduceDriver(mapper, reducer)
                               .withKeyGroupingComparator(groupingComparator)
                               .withKeyOrderComparator(sortComparator);
        driver.setConfiguration(job.getConfiguration());
        return driver;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String setInputPath(String[] arg0) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String setOutputPath(String[] arg0) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setup() throws Exception {
    }
}
