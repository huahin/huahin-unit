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
import java.util.ArrayList;
import java.util.List;

import org.huahinframework.core.Filter;
import org.huahinframework.core.Summarizer;
import org.huahinframework.core.io.Record;
import org.huahinframework.core.writer.Writer;
import org.huahinframework.unit.JobDriver;
import org.junit.Test;

/**
 *
 */
public class JobDriverSortTest extends JobDriver {
    private static final String LABEL_COLUMN = "COLUMN";
    private static final String LABEL_VALUE = "VALUE";

    private static final String COLUMN_A = "A";
    private static final String COLUMN_B = "B";
    private static final String COLUMN_C = "C";

    public static class TestFilter extends Filter {
        @Override
        public void filter(Record record, Writer writer)
                throws IOException, InterruptedException {
            int value = record.getValueInteger(LABEL_VALUE);
            Record emitRecord = new Record();
            emitRecord.addGrouping(LABEL_COLUMN, record.getGroupingString(LABEL_COLUMN));
            emitRecord.addSort(value, Record.SORT_LOWER, 1);
            emitRecord.addValue(LABEL_COLUMN, record.getValueString(LABEL_COLUMN));
            emitRecord.addValue(LABEL_VALUE, value);
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
            while (hasNext()) {
                writer.write(next(writer));
            }
        }

        @Override
        public void summarizerSetup() {
        }
    }

    @Test
    public void test()
            throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        addJob().setFilter(TestFilter.class)
                .setSummarizer(TestSummarizer.class);

        Record r1 = new Record();
        r1.addGrouping(LABEL_COLUMN, COLUMN_A);
        r1.addValue(LABEL_COLUMN, COLUMN_C);
        r1.addValue(LABEL_VALUE, 6);

        Record r2 = new Record();
        r2.addGrouping(LABEL_COLUMN, COLUMN_A);
        r2.addValue(LABEL_COLUMN, COLUMN_B);
        r2.addValue(LABEL_VALUE, 3);

        Record r3 = new Record();
        r3.addGrouping(LABEL_COLUMN, COLUMN_A);
        r3.addValue(LABEL_COLUMN, COLUMN_A);
        r3.addValue(LABEL_VALUE, 1);

        List<Record> input = new ArrayList<Record>();
        input.add(r1);
        input.add(r2);
        input.add(r3);

        List<Record> output = new ArrayList<Record>();
        output.add(r3);
        output.add(r2);
        output.add(r1);

        run(input, output, true);
    }
}
