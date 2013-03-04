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
import org.huahinframework.core.SimpleJob;
import org.huahinframework.core.Summarizer;
import org.huahinframework.core.io.Record;
import org.huahinframework.core.util.StringUtil;
import org.huahinframework.core.writer.Writer;
import org.junit.Test;

/**
 *
 */
public class JobDriverConfTest extends JobDriver {
    private static final String LABEL_COLUMN = "COLUMN";
    private static final String LABEL_VALUE = "VALUE";

    private static final String COLUMN_A = "A";

    private static final String RUN = "RUN";

    private static final String[] LABELS = new String[] { LABEL_COLUMN, LABEL_VALUE };

    public static class TestFilter extends Filter {
        private boolean run;

        @Override
        public void init() {
        }

        @Override
        public void filter(Record record, Writer writer)
                throws IOException, InterruptedException {
            if (!run) {
                return;
            }

            Record emitRecord = new Record();
            emitRecord.addGrouping(LABEL_COLUMN, record.getValueString(LABEL_COLUMN));
            emitRecord.addValue(LABEL_VALUE,
                                Integer.valueOf(record.getValueString(LABEL_VALUE)));
            writer.write(emitRecord);
        }

        @Override
        public void filterSetup() {
            run = getBooleanParameter(RUN);
        }
    }

    public static class TestSummarizer extends Summarizer {
        private boolean run;

        @Override
        public void init() {
        }

        @Override
        public void summarize(Writer writer)
                throws IOException, InterruptedException {
            if (hasNext() && run) {
                writer.write(next(writer));
            }
        }

        @Override
        public void summarizerSetup() {
            run = getBooleanParameter(RUN);
        }
    }

    @Test
    public void testRun()
            throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, URISyntaxException {
        SimpleJob j = addJob(LABELS, StringUtil.TAB, false).setFilter(TestFilter.class)
                                                           .setSummarizer(TestSummarizer.class);

        List<String> input = new ArrayList<String>();
        input.add(COLUMN_A + StringUtil.TAB + 1);

        List<Record> output = new ArrayList<Record>();
        Record r1 = new Record();
        r1.addGrouping(LABEL_COLUMN, COLUMN_A);
        r1.addValue(LABEL_VALUE, 1);
        output.add(r1);

        j.setParameter(RUN, true);

        run(input, output, true);
    }

    @Test
    public void testNotRun()
            throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, URISyntaxException {
        addJob(LABELS, StringUtil.TAB, false).setFilter(TestFilter.class)
                                             .setSummarizer(TestSummarizer.class);

        List<String> input = new ArrayList<String>();
        input.add(COLUMN_A + StringUtil.TAB + 1);

        run(input, new ArrayList<Record>(), true);
    }
}
