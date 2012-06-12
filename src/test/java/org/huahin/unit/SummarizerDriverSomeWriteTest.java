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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.huahin.core.Summarizer;
import org.huahin.core.Writer;
import org.huahin.core.io.Record;
import org.junit.Test;

/**
 *
 */
public class SummarizerDriverSomeWriteTest extends SummarizerDriver {
    private static final String LABEL_COLUMN = "COLUMN";
    private static final String LABEL_VALUE = "VALUE";

    private static final String COLUMN_A = "A";

    private static class TestEndSummarizer extends Summarizer {
        @Override
        public void init() {
        }

        @Override
        public void summarizer(Writer writer)
                throws IOException, InterruptedException {
            int count = 0;
            while (hasNext()) {
                Record record = next(writer);
                if (count > 2) {
                    break;
                }

                writer.write(record);
                count++;
            }
        }

        @Override
        public void summarizerSetup() {
        }
    }

    @Test
    public void test() {
        List<Record> input = new ArrayList<Record>();
        for (int i = 1; i <= 5; i++) {
            Record r = new Record();
            r.addGrouping(LABEL_COLUMN, COLUMN_A);
            r.addValue(LABEL_VALUE, i * (i + 1));
            input.add(r);
        }

        List<Record> output = new ArrayList<Record>();
        for (int i = 0; i < 3; i++) {
            output.add(input.get(i));
        }

        run(input, output);
    }

    @Override
    public Summarizer getSummarizer() {
        return new TestEndSummarizer();
    }
}
