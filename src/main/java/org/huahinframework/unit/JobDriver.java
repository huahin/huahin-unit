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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.huahinframework.core.SimpleJob;
import org.huahinframework.core.SimpleJobTool;
import org.huahinframework.core.io.Key;
import org.huahinframework.core.io.Record;
import org.huahinframework.core.io.Value;
import org.huahinframework.core.lib.input.creator.JoinRegexValueCreator;
import org.huahinframework.core.lib.input.creator.JoinValueCreator;
import org.huahinframework.core.lib.input.creator.LabelValueCreator;
import org.huahinframework.core.lib.input.creator.SimpleValueCreator;
import org.huahinframework.core.lib.input.creator.ValueCreator;
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

    private String masterSeparator;
    private String[] masterLabels;
    private String masterColumn;
    private String dataColumn;
    private boolean regex;
    private List<String> masterData;

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
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void run(List<?> input, List<Record> output, boolean stdout)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {
        if (input == null || input.isEmpty()) {
            fail("input is empty");
        }

        List<Pair<WritableComparable, Writable>> actual = null;
        Object o = input.get(0);
        if (o instanceof String) {
            actual = runString((List<String>) input);
        } else if (o instanceof Record) {
            actual = runRecord((List<Record>) input);
        } else {
            fail("input object is miss match");
        }

        if (stdout) {
            for (Pair<WritableComparable, Writable> pair : actual) {
                System.out.println(pair);
            }
        }

        assertEquals(output.size(), actual.size());

        if (output != null) {
            for (int i = 0; i < output.size(); i++) {
                Pair<WritableComparable, Writable> pair = actual.get(i);
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
    @SuppressWarnings("rawtypes")
    private List<Pair<WritableComparable, Writable>> runString(List<? extends String> input)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {
        boolean first = true;
        List<Pair<WritableComparable, Writable>> actual = null;
        String[] beforeSummarizerOutputLabel = null;

        for (Job job : sequencalJobChain.getJobs()) {
            MapReduceDriver<WritableComparable, Writable, WritableComparable, Writable, WritableComparable, Writable> driver = createDriver(job, false);

            if (first) {
                boolean formatIgnored =
                        sequencalJobChain.getJobs().get(0).getConfiguration().getBoolean(SimpleJob.FORMAT_IGNORED, false);
                String[] labels =
                        sequencalJobChain.getJobs().get(0).getConfiguration().getStrings(SimpleJob.LABELS);
                String separator =
                        sequencalJobChain.getJobs().get(0).getConfiguration().get(SimpleJob.SEPARATOR, StringUtil.COMMA);
                boolean separatorRegex =
                        sequencalJobChain.getJobs().get(0).getConfiguration().getBoolean(SimpleJob.SEPARATOR_REGEX, false);

                if (masterSeparator == null) {
                    masterSeparator = separator;
                }

                for (String s : input) {
                    Key key = new Key();
                    key.addPrimitiveValue("KEY", 1L);
                    Value value = new Value();

                    ValueCreator valueCreator = null;
                    if (labels == null) {
                        valueCreator = new SimpleValueCreator(separator, separatorRegex);
                    } else {
                        if (masterData == null) {
                            valueCreator = new LabelValueCreator(labels, formatIgnored, separator, separatorRegex);
                        } else {
                            int masterJoinNo = getJoinNo(masterLabels, masterColumn);
                            int dataJoinNo = getJoinNo(labels, dataColumn);

                            Map<String, String[]> simpleJoinMap = null;
                            simpleJoinMap =
                                    getSimpleMaster(masterData, masterJoinNo, masterSeparator);
                            if (regex) {
                                Map<Pattern, String[]> simpleJoinRegexMap = new HashMap<Pattern, String[]>();
                                for (Entry<String, String[]> entry : simpleJoinMap.entrySet()) {
                                    Pattern p = Pattern.compile(entry.getKey());
                                    simpleJoinRegexMap.put(p, entry.getValue());
                                }
                                valueCreator =
                                        new JoinRegexValueCreator(labels, formatIgnored, separator, separatorRegex, masterLabels,
                                                                 masterJoinNo, dataJoinNo, simpleJoinRegexMap);
                            } else {
                                valueCreator = new JoinValueCreator(labels, formatIgnored, separator, separatorRegex, masterLabels,
                                                                    masterJoinNo, dataJoinNo, simpleJoinMap);
                            }
                        }
                    }

                    valueCreator.create(s, value);
                    driver.addInput(key, value);
                }

                if (job instanceof SimpleJob) {
                    SimpleJob sj = (SimpleJob) job;
                    String[] summarizerOutputLabels = sj.getSummarizerOutputLabels();
                    if (summarizerOutputLabels != null) {
                        beforeSummarizerOutputLabel = summarizerOutputLabels;
                    }
                }

                first = false;
            } else {
                for (Pair<WritableComparable, Writable> pair : actual) {
                    driver.addInput(pair.getFirst(), pair.getSecond());
                }

                if (job instanceof SimpleJob) {
                    SimpleJob sj = (SimpleJob) job;
                    if (!sj.isNatural()) {
                        if (beforeSummarizerOutputLabel != null) {
                            job.getConfiguration().setStrings(SimpleJob.BEFORE_SUMMARIZER_OUTPUT_LABELS,
                                                              beforeSummarizerOutputLabel);
                        }
                        String[] summarizerOutputLabels = sj.getSummarizerOutputLabels();
                        if (summarizerOutputLabels != null) {
                            beforeSummarizerOutputLabel = summarizerOutputLabels;
                        }
                    }
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
    @SuppressWarnings("rawtypes")
    private List<Pair<WritableComparable, Writable>> runRecord(List<? extends Record> input)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {
        boolean first = true;
        List<Pair<WritableComparable, Writable>> actual = null;
        String[] beforeSummarizerOutputLabel = null;

        for (Job job : sequencalJobChain.getJobs()) {
            MapReduceDriver<WritableComparable, Writable, WritableComparable, Writable, WritableComparable, Writable> driver = createDriver(job, true);

            if (first) {
                for (Record r : input) {
                    driver.addInput(r.getKey(), r.getValue());
                }

                if (job instanceof SimpleJob) {
                    SimpleJob sj = (SimpleJob) job;
                    String[] summarizerOutputLabels = sj.getSummarizerOutputLabels();
                    if (summarizerOutputLabels != null) {
                        beforeSummarizerOutputLabel = summarizerOutputLabels;
                    }
                }

                first = false;
            } else {
                for (Pair<WritableComparable, Writable> pair : actual) {
                    driver.addInput(pair.getFirst(), pair.getSecond());
                }

                if (job instanceof SimpleJob) {
                    SimpleJob sj = (SimpleJob) job;
                    if (!sj.isNatural()) {
                        if (beforeSummarizerOutputLabel != null) {
                            job.getConfiguration().setStrings(SimpleJob.BEFORE_SUMMARIZER_OUTPUT_LABELS,
                                                              beforeSummarizerOutputLabel);
                        }
                        String[] summarizerOutputLabels = sj.getSummarizerOutputLabels();
                        if (summarizerOutputLabels != null) {
                            beforeSummarizerOutputLabel = summarizerOutputLabels;
                        }
                    }
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
    private MapReduceDriver<WritableComparable, Writable, WritableComparable, Writable, WritableComparable, Writable> createDriver(Job job, boolean record)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        Mapper mapper = job.getMapperClass().newInstance();
        Reducer reducer = job.getReducerClass().newInstance();
        RawComparator groupingComparator = job.getGroupingComparator();
        RawComparator sortComparator = job.getSortComparator();
        MapReduceDriver<WritableComparable, Writable, WritableComparable, Writable, WritableComparable, Writable> driver =
                MapReduceDriver.newMapReduceDriver(mapper, reducer)
                               .withKeyGroupingComparator(groupingComparator)
                               .withKeyOrderComparator(sortComparator);
        driver.setConfiguration(job.getConfiguration());
        return driver;
    }


    /**
     * Easily supports the Join. To use the setSimpleJoin,
     * you must be a size master data appear in the memory of the task.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param masterData master data
     */
    protected void setSimpleJoin(String[] masterLabels, String masterColumn,
                                 String dataColumn, List<String> masterData) {
        masterSeparator = null;
        setSimpleJoin(masterLabels, masterColumn, dataColumn, masterSeparator, false, masterData);
    }

    /**
     * Easily supports the Join. To use the setSimpleJoin,
     * you must be a size master data appear in the memory of the task.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param regex master join is regex;
     * @param masterData master data
     */
    protected void setSimpleJoin(String[] masterLabels, String masterColumn,
                                 String dataColumn, boolean regex, List<String> masterData) {
        masterSeparator = null;
        setSimpleJoin(masterLabels, masterColumn, dataColumn, masterSeparator, regex, masterData);
    }

    /**
     * Easily supports the Join. To use the setSimpleJoin,
     * you must be a size master data appear in the memory of the task.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param masterSeparator separator
     * @param regex master join is regex
     * @param masterData master data
     */
    protected void setSimpleJoin(String[] masterLabels, String masterColumn, String dataColumn,
                                 String masterSeparator, boolean regex, List<String> masterData) {
        this.masterLabels = masterLabels;
        this.masterColumn = masterColumn;
        this.dataColumn = dataColumn;
        this.masterSeparator = masterSeparator;
        this.regex = regex;
        this.masterData = masterData;
    }

    /**
     * get join column number
     * @param labels label's
     * @param join join column
     * @return join column number
     */
    private int getJoinNo(String[] labels, String join) {
        for (int i = 0; i < labels.length; i++) {
            if (join.equals(labels[i])) {
                return i;
            }
        }
        return -1;
    }

    /**
     * @param masterData
     * @param joinColumnNo
     * @param masterSeparator
     * @return master map
     */
    private Map<String, String[]> getSimpleMaster(List<String> masterData,
                                                  int joinColumnNo,
                                                  String separator) {
        Map<String, String[]> m = new HashMap<String, String[]>();
        for (String line : masterData) {
            String[] strings = StringUtil.split(line, separator, false);
            if (masterLabels.length != strings.length) {
                continue;
            }

            String joinData = strings[joinColumnNo];
            String[] data = new String[strings.length];
            for (int i = 0; i < strings.length; i++) {
                data[i] = strings[i];
            }

            m.put(joinData, data);
        }
        return m;
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
