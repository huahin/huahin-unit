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
import java.util.List;

import org.huahinframework.core.Filter;
import org.huahinframework.core.Summarizer;
import org.huahinframework.core.io.Record;
import org.huahinframework.core.util.StringUtil;
import org.huahinframework.core.writer.Writer;
import org.junit.Test;

/**
 *
 */
public class JobDriverFileTest extends JobDriver {
    private static final String LABEL_COLUMN = "COLUMN";
    private static final String LABEL_VALUE = "VALUE";
    private static final String FILE_NAME = "FILE_NAME";
    private static final String FILE_LENGTH = "FILE_LENGTH";

    private static final String COLUMN_A = "A";

    private static final String[] LABELS = new String[] { LABEL_COLUMN, LABEL_VALUE };

    public static class TestFilter extends Filter {
        @Override
        public void filter(Record record, Writer writer)
                throws IOException, InterruptedException {
            Record emitRecord = new Record();
            emitRecord.addGrouping(FILE_NAME, record.getGroupingString(FILE_NAME));
            emitRecord.addGrouping(FILE_LENGTH, record.getGroupingLong(FILE_LENGTH));
            emitRecord.addValue(LABEL_VALUE, Integer.valueOf(record.getValueString(LABEL_VALUE)));
            writer.write(emitRecord);
        }

        @Override
        public void filterSetup() {
        }

        @Override
        public void init() {
        }
    }

    public static class TestSummarizer extends Summarizer {
        @Override
        public void init() {
        }

        @Override
        public void summarize(Writer writer)
                throws IOException, InterruptedException {
            int count = 0;
            while (hasNext()) {
                Record record = next(writer);
                count += record.getValueInteger(LABEL_VALUE);
            }

            Record emitRecord = new Record();
            emitRecord.addValue(LABEL_VALUE, count);
            writer.write(emitRecord);
        }

        @Override
        public void summarizerSetup() {
        }
    }

    @Test
    public void test()
            throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, URISyntaxException {
        addJob(LABELS, StringUtil.TAB, false).setFilter(TestFilter.class)
                                             .setSummarizer(TestSummarizer.class);

        List<String> input = new ArrayList<String>();
        input.add(COLUMN_A + StringUtil.TAB + 1);
        input.add(COLUMN_A + StringUtil.TAB + 2);
        input.add(COLUMN_A + StringUtil.TAB + 3);

        List<Record> output = new ArrayList<Record>();
        Record r = new Record();
        r.addGrouping(FILE_NAME, "access_log");
        r.addGrouping(FILE_LENGTH, 256);
        r.addValue(LABEL_VALUE, 6);
        output.add(r);

        setFileName("access_log");
        setFileLength(256);
        run(input, output, true);
    }
}
