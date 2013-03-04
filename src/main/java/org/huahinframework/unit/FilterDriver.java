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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
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
import org.huahinframework.core.lib.input.creator.JoinSomeRegexValueCreator;
import org.huahinframework.core.lib.input.creator.JoinSomeValueCreator;
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

    protected Configuration conf;
    private List<String> masterData;
    private String masterSeparator;
    private String fileName;
    private long fileLength;

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
     * @throws URISyntaxException
     * @throws IOException
     */
    protected void run(String separator,
                       String input,
                       List<Record> output) throws IOException, URISyntaxException {
        run(null, separator, false, input, output);
    }

    /**
     * Run the test with this method.
     * The data is input for the {@link String}.
     * @param labels label of input data
     * @param separator separator of data
     * @param input input {@link String} data
     * @param output result of {@link Record} {@link List}
     * @throws URISyntaxException
     * @throws IOException
     */
    protected void run(String[] labels,
                       String separator,
                       String input,
                       List<Record> output) throws IOException, URISyntaxException {
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
     * @throws URISyntaxException
     * @throws IOException
     */
    public void run(String[] labels,
                    String separator,
                    boolean formatIgnored,
                    String input,
                    List<Record> output) throws IOException, URISyntaxException {
        run(labels, separator, false, formatIgnored, input, output);
    }

    /**
     * Run the test with this method.
     * The data is input for the {@link String}.
     * @param separator regex
     * @param input input {@link String} data
     * @param output result of {@link Record} {@link List}
     * @throws URISyntaxException
     * @throws IOException
     */
    protected void run(Pattern separator,
                       String input,
                       List<Record> output) throws IOException, URISyntaxException {
        run(null, separator.pattern(), true, false, input, output);
    }

    /**
     * Run the test with this method.
     * The data is input for the {@link String}.
     * @param labels label of input data
     * @param separator regex
     * @param input input {@link String} data
     * @param output result of {@link Record} {@link List}
     * @throws URISyntaxException
     * @throws IOException
     */
    protected void run(String[] labels,
                       Pattern separator,
                       String input,
                       List<Record> output) throws IOException, URISyntaxException {
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
     * @throws URISyntaxException
     * @throws IOException
     */
    public void run(String[] labels,
                    Pattern separator,
                    boolean formatIgnored,
                    String input,
                    List<Record> output) throws IOException, URISyntaxException {
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
     * @throws URISyntaxException
     * @throws IOException
     */
    public void run(String[] labels,
                    String separator,
                    boolean separatorRegex,
                    boolean formatIgnored,
                    String input,
                    List<Record> output) throws IOException, URISyntaxException {
        if (labels != null) {
            conf.setStrings(SimpleJob.LABELS, labels);
            if (conf.getInt(SimpleJob.READER_TYPE, -1) == -1) {
                conf.setInt(SimpleJob.READER_TYPE, SimpleJob.LABELS_READER);
            }
        } else {
            if (conf.getInt(SimpleJob.READER_TYPE, -1) == -1) {
                conf.setInt(SimpleJob.READER_TYPE, SimpleJob.SIMPLE_READER);
            }
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
            conf.set(SimpleJob.MASTER_SEPARATOR, masterSeparator);
        }

        Key key = new Key();
        key.addPrimitiveValue("KEY", 1L);
        if (fileName == null) {
            fileName = "exmpale.txt";
        }
        key.addPrimitiveValue("FILE_NAME", fileName);
        key.addPrimitiveValue("FILE_LENGTH", fileLength);
        Value value = new Value();

        ValueCreator valueCreator = null;
        int type = conf.getInt(SimpleJob.READER_TYPE, -1);
        switch (type) {
        case SimpleJob.SIMPLE_READER:
            valueCreator = new SimpleValueCreator(separator, separatorRegex);
            break;
        case SimpleJob.LABELS_READER:
            valueCreator = new LabelValueCreator(labels, formatIgnored, separator, separatorRegex);
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
    private Map<List<String>, String[]> getSimpleColumnsMaster(Configuration conf)
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
     * parameter setting.
     * @param name parameter name
     * @param value {@link String} parameter value
     */
    public void setParameter(String name, String value) {
        conf.set(name, value);
    }

    /**
     * parameter setting.
     * @param name parameter name
     * @param value boolean parameter value
     */
    public void setParameter(String name, String[] value) {
        conf.setStrings(name, value);
    }

    /**
     * parameter setting.
     * @param name parameter name
     * @param value @link String} array parameter value
     */
    public void setParameter(String name, boolean value) {
        conf.setBoolean(name, value);
    }

    /**
     * parameter setting.
     * @param name parameter name
     * @param value int parameter value
     */
    public void setParameter(String name, int value) {
        conf.setInt(name, value);
    }

    /**
     * parameter setting.
     * @param name parameter name
     * @param value long parameter value
     */
    public void setParameter(String name, long value) {
        conf.setLong(name, value);
    }

    /**
     * parameter setting.
     * @param name parameter name
     * @param value float parameter value
     */
    public void setParameter(String name, float value) {
        conf.setFloat(name, value);
    }

    /**
     * parameter setting.
     * @param name parameter name
     * @param value Enum parameter value
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void setParameter(String name, Enum value) {
        conf.setEnum(name, value);
    }

    /**
     * Set the {@link Filter} class in this method.
     * @return new {@link Filter}
     */
    public abstract Filter getFilter();

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
