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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.huahinframework.core.DataFormatException;
import org.huahinframework.core.Filter;
import org.huahinframework.core.SimpleJob;
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
    @SuppressWarnings("rawtypes")
    private Mapper<Key, Value, WritableComparable, Writable> mapper;

    @SuppressWarnings("rawtypes")
    private MapDriver<Key, Value, WritableComparable, Writable> driver;

    private Configuration conf;
    private String masterSeparator;
    private String[] masterLabels;
    private String masterColumn;
    private String dataColumn;
    private boolean regex;
    private List<String> masterData;

    /**
     * @throws java.lang.Exception
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Before
    public void setUp() throws Exception {
        mapper = (Mapper) getFilter();
        driver = new MapDriver<Key, Value, WritableComparable, Writable>(mapper);
        conf = new Configuration();
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
     * @param separator separator of data
     * @param input input {@link String} data
     * @param output result of {@link Record} {@link List}
     * @throws DataFormatException
     */
    protected void run(String separator,
                       String input,
                       List<Record> output) throws DataFormatException {
        run(null, separator, false, input, output);
    }

    /**
     * Run the test with this method.
     * The data is input for the {@link String}.
     * @param labels label of input data
     * @param separator separator of data
     * @param input input {@link String} data
     * @param output result of {@link Record} {@link List}
     * @throws DataFormatException
     */
    protected void run(String[] labels,
                       String separator,
                       String input,
                       List<Record> output) throws DataFormatException {
        run(labels, separator, false, input, output);
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
     * @throws DataFormatException
     */
    public void run(String[] labels,
                    String separator,
                    boolean formatIgnored,
                    String input,
                    List<Record> output) throws DataFormatException {
        run(labels, separator, false, formatIgnored, input, output);
    }

    /**
     * Run the test with this method.
     * The data is input for the {@link String}.
     * @param separator regex
     * @param input input {@link String} data
     * @param output result of {@link Record} {@link List}
     * @throws DataFormatException
     */
    protected void run(Pattern separator,
                       String input,
                       List<Record> output) throws DataFormatException {
        run(null, separator.pattern(), true, false, input, output);
    }

    /**
     * Run the test with this method.
     * The data is input for the {@link String}.
     * @param labels label of input data
     * @param separator regex
     * @param input input {@link String} data
     * @param output result of {@link Record} {@link List}
     * @throws DataFormatException
     */
    protected void run(String[] labels,
                       Pattern separator,
                       String input,
                       List<Record> output) throws DataFormatException {
        run(labels, separator.pattern(), true, false, input, output);
    }

    /**
     * Run the test with this method.
     * The data is input for the {@link String}.
     * @param labels label of input data
     * @param separator regex
     * @param formatIgnored
     * If true, {@link DataFormatException} will be throw if there is a format error.
     * If false is ignored (default).
     * @param input input {@link String} data
     * @param output result of {@link Record} {@link List}
     * @throws DataFormatException
     */
    public void run(String[] labels,
                    Pattern separator,
                    boolean formatIgnored,
                    String input,
                    List<Record> output) throws DataFormatException {
        run(labels, separator.pattern(), true, formatIgnored, input, output);
    }

    /**
     * Run the test with this method.
     * The data is input for the {@link String}.
     * @param labels label of input data
     * @param separator separator of data
     * @param separatorRegex separator is regex
     * @param formatIgnored
     * If true, {@link DataFormatException} will be throw if there is a format error.
     * If false is ignored (default).
     * @param input input {@link String} data
     * @param output result of {@link Record} {@link List}
     * @throws DataFormatException
     */
    public void run(String[] labels,
                    String separator,
                    boolean separatorRegex,
                    boolean formatIgnored,
                    String input,
                    List<Record> output) throws DataFormatException {
        if (labels != null) {
            conf.setStrings(SimpleJob.LABELS, labels);
        }
        if (separator == null || separator.isEmpty()) {
            separator = StringUtil.COMMA;
        }

        conf.set(SimpleJob.SEPARATOR, separator);
        if (separatorRegex) {
            conf.setBoolean(SimpleJob.SEPARATOR_REGEX, true);
        }
        conf.setBoolean(SimpleJob.FORMAT_IGNORED, formatIgnored);
        driver.setConfiguration(conf);

        separator = separator == null ? StringUtil.COMMA : separator;
        if (masterSeparator == null) {
            masterSeparator = separator;
        }

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

        valueCreator.create(input, value);
        driver.withInput(key, value);

        if (output != null) {
            for (Record r : output) {
                driver.withOutput(r.isGroupingNothing() ? NullWritable.get() : r.getKey(),
                                  r.isValueNothing() ? NullWritable.get() : r.getValue());
            }
        }

        driver.runTest();
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
     * parameter setting.
     * @param name parameter name
     * @param value {@link String} parameter value
     */
    protected void setParameter(String name, String value) {
        conf.set(name, value);
    }

    /**
     * parameter setting
     * @param name parameter name
     * @param value boolean parameter value
     */
    protected void setParameter(String name, boolean value) {
        conf.setBoolean(name, value);
    }

    /**
     * parameter setting.
     * @param name parameter name
     * @param value int parameter value
     */
    protected void setParameter(String name, int value) {
        conf.setInt(name, value);
    }

    /**
     * Set the {@link Filter} class in this method.
     * @return new {@link Filter}
     */
    public abstract Filter getFilter();
}
