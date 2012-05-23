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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.huahin.core.SimpleJob;
import org.huahin.core.SimpleJobTool;
import org.huahin.core.io.Key;
import org.huahin.core.io.Record;
import org.huahin.core.io.Value;
import org.huahin.core.util.StringUtil;
import org.junit.Before;

/**
 *
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
     *
     * @param input
     * @param output
     * @param stdout
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public void run(List<? extends Object> input, List<Record> output, boolean stdout)
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

        for (SimpleJob job : sequencalJobChain.getJobs()) {
            MapReduceDriver<Writable, Writable, Key, Value, Key, Value> driver = createDriver(job, false);

            if (first) {
                final LongWritable one = new LongWritable(1L);
                for (String s : input) {
                    driver.addInput(one, new Text(s));
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

        for (SimpleJob job : sequencalJobChain.getJobs()) {
            MapReduceDriver<Writable, Writable, Key, Value, Key, Value> driver = createDriver(job, true);

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
    private MapReduceDriver<Writable, Writable, Key, Value, Key, Value> createDriver(SimpleJob job, boolean record)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        if (record) {
            job.getConfiguration().setBoolean(SimpleJob.FIRST, false);
        }
        Mapper mapper = job.getMapperClass().newInstance();
        Reducer reducer = job.getReducerClass().newInstance();
        RawComparator groupingComparator = job.getGroupingComparator();
        RawComparator sortComparator = job.getSortComparator();
        MapReduceDriver<Writable, Writable, Key, Value, Key, Value> driver =
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
