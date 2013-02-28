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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
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
import org.huahinframework.core.lib.input.creator.JoinRegexValueCreator;
import org.huahinframework.core.lib.input.creator.JoinSomeRegexValueCreator;
import org.huahinframework.core.lib.input.creator.JoinSomeValueCreator;
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
    private List<String> masterData;
    private boolean bigJoin;
    private String fileName;
    private long fileLength;

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
     * @throws URISyntaxException
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void run(List<?> input, List<Record> output, boolean stdout)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, URISyntaxException {
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
     * @throws URISyntaxException
     */
    @SuppressWarnings("rawtypes")
    private List<Pair<WritableComparable, Writable>> runString(List<? extends String> input)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, URISyntaxException {
        boolean first = true;
        List<Pair<WritableComparable, Writable>> actual = null;
        String[] beforeSummarizerOutputLabel = null;

        for (Job job : sequencalJobChain.getJobs()) {
            MapReduceDriver<WritableComparable,
                            Writable,
                            WritableComparable,
                            Writable,
                            WritableComparable,
                            Writable> driver = createDriver(job);

            boolean onlyJoin = false;
            if (first) {
                boolean formatIgnored =
                        sequencalJobChain.getJobs().get(0).getConfiguration().getBoolean(SimpleJob.FORMAT_IGNORED, false);
                String[] labels =
                        sequencalJobChain.getJobs().get(0).getConfiguration().getStrings(SimpleJob.LABELS);
                String separator =
                        sequencalJobChain.getJobs().get(0).getConfiguration().get(SimpleJob.SEPARATOR, StringUtil.COMMA);

                if (masterSeparator == null) {
                    masterSeparator = separator;
                    conf.set(SimpleJob.MASTER_SEPARATOR, masterSeparator);
                }

                if (labels != null) {
                    if (conf.getInt(SimpleJob.READER_TYPE, -1) == -1) {
                        conf.setInt(SimpleJob.READER_TYPE, SimpleJob.LABELS_READER);
                    }
                } else {
                    if (conf.getInt(SimpleJob.READER_TYPE, -1) == -1) {
                        conf.setInt(SimpleJob.READER_TYPE, SimpleJob.SIMPLE_READER);
                    }
                }

                Key key = new Key();
                key.addPrimitiveValue("KEY", 1L);
                if (fileName == null) {
                    fileName = "exmpale.txt";
                }
                key.addPrimitiveValue("FILE_NAME", fileName);
                key.addPrimitiveValue("FILE_LENGTH", fileLength);

                ValueCreator valueCreator = null;
                int type = conf.getInt(SimpleJob.READER_TYPE, -1);
                switch (type) {
                case SimpleJob.SIMPLE_READER:
                    valueCreator = new SimpleValueCreator(separator, conf.getBoolean(SimpleJob.SEPARATOR_REGEX, false));
                    break;
                case SimpleJob.LABELS_READER:
                    valueCreator = new LabelValueCreator(labels, formatIgnored, separator, conf.getBoolean(SimpleJob.SEPARATOR_REGEX, false));
                    break;
                case SimpleJob.SINGLE_COLUMN_JOIN_READER:
                    Map<String, String[]> simpleJoinMap = getSimpleMaster(conf);
                    if (!conf.getBoolean(SimpleJob.JOIN_REGEX, false)) {
                        valueCreator = new JoinValueCreator(labels, formatIgnored, separator,
                                                            formatIgnored, simpleJoinMap, conf);
                    } else {
                        Map<Pattern, String[]> simpleJoinRegexMap = new HashMap<Pattern, String[]>();
                        for (Entry<String, String[]> entry : simpleJoinMap.entrySet()) {
                            Pattern p = Pattern.compile(entry.getKey());
                            simpleJoinRegexMap.put(p, entry.getValue());
                        }
                        valueCreator = new JoinRegexValueCreator(labels, formatIgnored, separator,
                                                                 formatIgnored, simpleJoinRegexMap, conf);
                    }
                    break;
                case SimpleJob.SOME_COLUMN_JOIN_READER:
                    Map<List<String>, String[]> simpleColumnsJoinMap = getSimpleColumnsMaster(conf);
                    if (!conf.getBoolean(SimpleJob.JOIN_REGEX, false)) {
                        valueCreator = new JoinSomeValueCreator(labels, formatIgnored, separator,
                                                                formatIgnored, simpleColumnsJoinMap, conf);
                    } else {
                        Map<List<Pattern>, String[]> simpleColumnsJoinRegexMap = new HashMap<List<Pattern>, String[]>();
                        for (Entry<List<String>, String[]> entry : simpleColumnsJoinMap.entrySet()) {
                            List<String> l = entry.getKey();
                            List<Pattern> p = new ArrayList<Pattern>();
                            for (String s : l) {
                                p.add(Pattern.compile(s));
                            }
                            simpleColumnsJoinRegexMap.put(p, entry.getValue());
                        }
                        valueCreator = new JoinSomeRegexValueCreator(labels, formatIgnored, separator,
                                                                     formatIgnored, simpleColumnsJoinRegexMap, conf);
                    }
                    break;
                default:
                    break;
                }

                if (bigJoin && sequencalJobChain.getJobs().size() == 1) {
                    SimpleJob sj = (SimpleJob) job;
                    if (!sj.isMapper() && !sj.isReducer()) {
                        actual = new ArrayList<Pair<WritableComparable, Writable>>();
                        String[] masterLabels = conf.getStrings(SimpleJob.MASTER_LABELS);
                        for (String s : input) {
                            Value value = new Value();
                            valueCreator.create(s, value);
                            Key k = new Key();
                            for (String label : labels) {
                                k.addPrimitiveValue(label, value.getPrimitiveValue(label));
                            }

                            String[] masterData = null;
                            if (type == SimpleJob.SINGLE_COLUMN_JOIN_READER) {
                                Map<String, String[]> simpleJoinMap = getSimpleMaster(conf);
                                masterData = simpleJoinMap.get(value.getPrimitiveValue(conf.get(SimpleJob.JOIN_MASTER_COLUMN)));
                            } else {
                                Map<List<String>, String[]> simpleColumnsJoinMap = getSimpleColumnsMaster(conf);
                                List<String> l = new ArrayList<String>();
                                for (String data : conf.getStrings(SimpleJob.JOIN_MASTER_COLUMN)) {
                                    l.add((String) value.getPrimitiveValue(data));
                                }
                                masterData = simpleColumnsJoinMap.get(l);
                            }

                            if (masterData == null) {
                                masterData = new String[masterLabels.length];
                            }
                            for (int i = 0; i < masterData.length; i++) {
                                k.addPrimitiveValue(masterLabels[i], masterData[i]);
                            }

                            Pair<WritableComparable, Writable> p =
                                    new Pair<WritableComparable, Writable>(k, new Text());
                            actual.add(p);
                        }
                        onlyJoin = true;
                    }
                }

                for (String s : input) {
                    Value value = new Value();
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

            if (!onlyJoin) {
                actual = driver.run();
            }
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
            MapReduceDriver<WritableComparable,
                            Writable,
                            WritableComparable,
                            Writable,
                            WritableComparable,
                            Writable> driver = createDriver(job);

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
     * @return List<Pair<Key, Value>>
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private MapReduceDriver<WritableComparable, Writable, WritableComparable, Writable, WritableComparable, Writable> createDriver(Job job)
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
        setSimpleJoin(masterLabels, masterColumn, dataColumn, null, false, masterData);
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
        setSimpleJoin(masterLabels, masterColumn, dataColumn, null, regex, masterData);
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
        this.conf.setInt(SimpleJob.READER_TYPE, SimpleJob.SINGLE_COLUMN_JOIN_READER);
        this.conf.setStrings(SimpleJob.MASTER_LABELS, masterLabels);
        this.conf.set(SimpleJob.JOIN_MASTER_COLUMN, masterColumn);
        this.conf.set(SimpleJob.JOIN_DATA_COLUMN, dataColumn);
        this.masterSeparator = masterSeparator;
        this.conf.setBoolean(SimpleJob.JOIN_REGEX, regex);
        this.masterData = masterData;
    }

    /**
     * Easily supports the Join. To use the setSimpleJoin,
     * you must be a size master data appear in the memory of the task.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param masterData master data
     * @throws DataFormatException
     */
    protected void setSimpleJoin(String[] masterLabels, String[] masterColumn,
                                 String[] dataColumn, List<String> masterData)
                                         throws DataFormatException {
        setSimpleJoin(masterLabels, masterColumn, dataColumn, null, false, masterData);
    }

    /**
     * Easily supports the Join. To use the setSimpleJoin,
     * you must be a size master data appear in the memory of the task.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param regex master join is regex;
     * @param masterData master data
     * @throws DataFormatException
     */
    protected void setSimpleJoin(String[] masterLabels, String[] masterColumn,
                                 String[] dataColumn, boolean regex, List<String> masterData)
                                         throws DataFormatException {
        setSimpleJoin(masterLabels, masterColumn, dataColumn, null, regex, masterData);
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
     * @throws DataFormatException
     */
    protected void setSimpleJoin(String[] masterLabels, String[] masterColumn, String[] dataColumn,
                                 String masterSeparator, boolean regex, List<String> masterData)
                                         throws DataFormatException {
        if (masterColumn.length != dataColumn.length) {
            throw new DataFormatException("masterColumns and dataColumns lenght is miss match.");
        }

        this.conf.setInt(SimpleJob.READER_TYPE, SimpleJob.SOME_COLUMN_JOIN_READER);
        this.conf.setStrings(SimpleJob.MASTER_LABELS, masterLabels);
        this.conf.setStrings(SimpleJob.JOIN_MASTER_COLUMN, masterColumn);
        this.conf.setStrings(SimpleJob.JOIN_DATA_COLUMN, dataColumn);
        this.masterSeparator = masterSeparator;
        this.conf.setBoolean(SimpleJob.JOIN_REGEX, regex);
        this.masterData = masterData;
    }

    /**
     * to join the data that does not fit into memory.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param masterData master data
     */
    protected void setBigJoin(String[] masterLabels, String masterColumn,
                              String dataColumn, List<String> masterData) {
        setBigJoin(masterLabels, masterColumn, dataColumn, null, masterData);
    }

    /**
     * to join the data that does not fit into memory.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param masterSeparator separator
     * @param masterData master data
     */
    protected void setBigJoin(String[] masterLabels, String masterColumn,
                              String dataColumn, String masterSeparator, List<String> masterData) {
        this.conf.setInt(SimpleJob.READER_TYPE, SimpleJob.SINGLE_COLUMN_JOIN_READER);
        this.conf.setStrings(SimpleJob.MASTER_LABELS, masterLabels);
        this.conf.set(SimpleJob.JOIN_MASTER_COLUMN, masterColumn);
        this.conf.set(SimpleJob.JOIN_DATA_COLUMN, dataColumn);
        this.masterSeparator = masterSeparator;
        this.masterData = masterData;
        this.bigJoin = true;
    }

    /**
     * to join the data that does not fit into memory.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param masterData master data
     * @throws DataFormatException
     */
    protected void setBigJoin(String[] masterLabels, String[] masterColumn,
                              String[] dataColumn, List<String> masterData)
                                      throws DataFormatException {
        setBigJoin(masterLabels, masterColumn, dataColumn, null, masterData);
    }

    /**
     * to join the data that does not fit into memory.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param masterSeparator separator
     * @param masterData master data
     * @throws DataFormatException
     */
    protected void setBigJoin(String[] masterLabels, String[] masterColumn,
                              String[] dataColumn, String masterSeparator, List<String> masterData)
                                      throws DataFormatException {
        if (masterColumn.length != dataColumn.length) {
            throw new DataFormatException("masterColumns and dataColumns lenght is miss match.");
        }

        this.conf.setInt(SimpleJob.READER_TYPE, SimpleJob.SOME_COLUMN_JOIN_READER);
        this.conf.setStrings(SimpleJob.MASTER_LABELS, masterLabels);
        this.conf.setStrings(SimpleJob.JOIN_MASTER_COLUMN, masterColumn);
        this.conf.setStrings(SimpleJob.JOIN_DATA_COLUMN, dataColumn);
        this.masterSeparator = masterSeparator;
        this.masterData = masterData;
        this.bigJoin = true;
    }

    /**
     * This method is to determine automatically join the Simple and Big.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param masterData master data
     */
    protected void setJoin(String[] masterLabels, String masterColumn,
                           String dataColumn, List<String> masterData) {
        setJoin(masterLabels, masterColumn, dataColumn, null, false, masterData);
    }

    /**
     * This method is to determine automatically join the Simple and Big.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param regex master join is regex;
     * @param masterData master data
     */
    protected void setJoin(String[] masterLabels, String masterColumn,
                           String dataColumn, boolean regex, List<String> masterData) {
        setJoin(masterLabels, masterColumn, dataColumn, null, regex, masterData);
    }

    /**
     * This method is to determine automatically join the Simple and Big.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param masterSeparator separator
     * @param regex master join is regex
     * @param masterData master data
     */
    protected void setJoin(String[] masterLabels, String masterColumn, String dataColumn,
                           String masterSeparator, boolean regex, List<String> masterData) {
        setSimpleJoin(masterLabels, masterColumn, dataColumn, masterSeparator, regex, masterData);
    }

    /**
     * This method is to determine automatically join the Simple and Big.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param masterData master data
     * @throws DataFormatException
     */
    protected void setJoin(String[] masterLabels, String[] masterColumn,
                           String[] dataColumn, List<String> masterData)
                                         throws DataFormatException {
        setJoin(masterLabels, masterColumn, dataColumn, null, false, masterData);
    }

    /**
     * This method is to determine automatically join the Simple and Big.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param regex master join is regex;
     * @param masterData master data
     * @throws DataFormatException
     */
    protected void setJoin(String[] masterLabels, String[] masterColumn,
                                 String[] dataColumn, boolean regex, List<String> masterData)
                                         throws DataFormatException {
        setJoin(masterLabels, masterColumn, dataColumn, null, regex, masterData);
    }

    /**
     * This method is to determine automatically join the Simple and Big.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param masterSeparator separator
     * @param regex master join is regex
     * @param masterData master data
     * @throws DataFormatException
     */
    protected void setJoin(String[] masterLabels, String[] masterColumn, String[] dataColumn,
                           String masterSeparator, boolean regex, List<String> masterData)
                                         throws DataFormatException {
        setSimpleJoin(masterLabels, masterColumn, dataColumn, masterSeparator, regex, masterData);
    }

    /**
     * @param conf
     * @return Map
     * @throws IOException
     * @throws URISyntaxException
     */
    private Map<String, String[]> getSimpleMaster(Configuration conf)
            throws IOException, URISyntaxException {
        String path = conf.get(SimpleJob.MASTER_PATH);
        String[] masterLabels = conf.getStrings(SimpleJob.MASTER_LABELS);
        String separator = conf.get(SimpleJob.MASTER_SEPARATOR);
        String masterColumn = conf.get(SimpleJob.JOIN_MASTER_COLUMN);
        int joinColumnNo = StringUtil.getMatchNo(masterLabels, masterColumn);
        return getSimpleMaster(masterLabels, joinColumnNo, path, separator);
    }


    /**
     * @param masterLabels
     * @param joinColumnNo
     * @param path
     * @param separator
     * @return Map
     * @throws IOException
     */
    private Map<String, String[]> getSimpleMaster(String[] masterLabels,
                                                  int joinColumnNo,
                                                  String path,
                                                  String separator) throws IOException {
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
     * @param conf
     * @return Map
     * @throws IOException
     * @throws URISyntaxException
     */
    public Map<List<String>, String[]> getSimpleColumnsMaster(Configuration conf)
            throws IOException, URISyntaxException {
        String path = conf.get(SimpleJob.MASTER_PATH);
        String[] masterLabels = conf.getStrings(SimpleJob.MASTER_LABELS);
        String separator = conf.get(SimpleJob.MASTER_SEPARATOR);
        String[] masterColumn = conf.getStrings(SimpleJob.JOIN_MASTER_COLUMN);
        int[] joinColumnNo = StringUtil.getMatchNos(masterLabels, masterColumn);
        return getSimpleColumnsMaster(masterLabels, joinColumnNo, path, separator);
    }

    /**
     * @param masterLabels
     * @param joinColumnNo
     * @param path
     * @param separator
     * @return Map
     * @throws IOException
     * @throws URISyntaxException
     */
    private Map<List<String>, String[]> getSimpleColumnsMaster(String[] masterLabels,
                                                               int[] joinColumnNo,
                                                               String path,
                                                               String separator)
                                                                  throws IOException {
        Map<List<String>, String[]> m = new HashMap<List<String>, String[]>();

        for (String line : masterData) {
            String[] strings = StringUtil.split(line, separator, false);
            if (masterLabels.length != strings.length) {
                continue;
            }

            List<String> joinData = new ArrayList<String>();
            for (int i : joinColumnNo) {
                joinData.add(strings[i]);
            }

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

    /**
     * set input file name
     * @param fileName the fileName to set
     */
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    /**
     * set input file length
     * @param fileLength the fileLength to set
     */
    public void setFileLength(long fileLength) {
        this.fileLength = fileLength;
    }
}
