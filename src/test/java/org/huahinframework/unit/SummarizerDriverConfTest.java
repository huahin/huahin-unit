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
import java.util.Arrays;
import java.util.List;

import org.huahinframework.core.Summarizer;
import org.huahinframework.core.io.Record;
import org.huahinframework.core.writer.Writer;
import org.junit.Test;

/**
 *
 */
public class SummarizerDriverConfTest extends SummarizerDriver {
    private static final String LABEL_COLUMN = "COLUMN";
    private static final String LABEL_VALUE = "VALUE";

    private static final String COLUMN_A = "A";

    private static final String RUN = "RUN";

    private static class TestEndSummarizer extends Summarizer {
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
    public void testRun() {
        List<Record> input = new ArrayList<Record>();
        Record r = new Record();
        r.addGrouping(LABEL_COLUMN, COLUMN_A);
        r.addValue(LABEL_VALUE, 1);
        input.add(r);

        Record output = new Record();
        output.addGrouping(LABEL_COLUMN, COLUMN_A);
        output.addValue(LABEL_VALUE, 1);

        setParameter(RUN, true);

        run(input, Arrays.asList(output));
    }

    @Test
    public void testNotRun() {
        List<Record> input = new ArrayList<Record>();
        Record r = new Record();
        r.addGrouping(LABEL_COLUMN, COLUMN_A);
        r.addValue(LABEL_VALUE, 1);
        input.add(r);

        run(input, null);
    }

    @Override
    public Summarizer getSummarizer() {
        return new TestEndSummarizer();
    }
}
