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
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class JobDriverAutoJoinTest extends JobDriver {
    private static final String LABEL_ID = "ID";
    private static final String LABEL_VALUE = "VALUE";
    private static final String LABEL_NAME = "NAME";

    private static final String[] LABELS = { LABEL_ID, LABEL_VALUE };
    private static final String[] MASTER_LABELS = { LABEL_ID, LABEL_NAME };

    private List<String> masterData;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        masterData = createMaster();
    }

    public static class TestFilter extends Filter {
        @Override
        public void filter(Record record, Writer writer)
                throws IOException, InterruptedException {
            String name = record.getValueString(LABEL_NAME);
            if (name == null) {
                return;
            }

            Record emitRecord = new Record();
            emitRecord.addGrouping(LABEL_NAME, name);
            emitRecord.addValue(
                    LABEL_VALUE,
                    Integer.valueOf(record.getValueString(LABEL_VALUE)));
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
        input.add(1 + StringUtil.TAB + 1);
        input.add(1 + StringUtil.TAB + 2);
        input.add(1 + StringUtil.TAB + 3);
        input.add(6 + StringUtil.TAB + 1);
        input.add(6 + StringUtil.TAB + 2);
        input.add(11 + StringUtil.TAB + 1);

        List<Record> output = new ArrayList<Record>();
        Record r1 = new Record();
        r1.addGrouping(LABEL_NAME, "NAME_1");
        r1.addValue(LABEL_VALUE, 6);
        output.add(r1);

        Record r2 = new Record();
        r2.addGrouping(LABEL_NAME, "NAME_6");
        r2.addValue(LABEL_VALUE, 3);
        output.add(r2);

        setJoin(MASTER_LABELS, LABEL_ID, LABEL_ID, masterData);

        run(input, output, true);
    }

    private List<String> createMaster() {
        List<String> l = new ArrayList<String>();
        for (int i = 1; i <= 10; i++) {
            l.add(i + StringUtil.TAB + "NAME_" + i);
        }
        return l;
    }
}
