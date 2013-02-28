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
import java.util.Arrays;

import org.huahinframework.core.Filter;
import org.huahinframework.core.io.Record;
import org.huahinframework.core.util.StringUtil;
import org.huahinframework.core.writer.Writer;
import org.junit.Test;

/**
 *
 */
public class FilterDriverFileTest extends FilterDriver {
    private static final String LABEL_COLUMN = "COLUMN";
    private static final String LABEL_VALUE = "VALUE";
    private static final String FILE_NAME = "FILE_NAME";
    private static final String FILE_LENGTH = "FILE_LENGTH";

    private static final String COLUMN_A = "A";

    private static final String[] LABELS = { LABEL_COLUMN, LABEL_VALUE };

    private static class TestFilter extends Filter {
        @Override
        public void filter(Record record, Writer writer)
                throws IOException, InterruptedException {
            Record emitRecord = new Record();
            emitRecord.addGrouping(FILE_NAME, record.getGroupingString(FILE_NAME));
            emitRecord.addGrouping(FILE_LENGTH, record.getGroupingLong(FILE_LENGTH));
            emitRecord.addValue(LABEL_COLUMN, record.getValueString(LABEL_COLUMN));
            emitRecord.addValue(LABEL_VALUE, record.getValueString(LABEL_VALUE));
            writer.write(emitRecord);
        }

        @Override
        public void filterSetup() {
        }

        @Override
        public void init() {
        }
    }

    @Test
    public void testHit() throws IOException, URISyntaxException {
        String input = COLUMN_A + StringUtil.TAB + 1;

        Record output = new Record();
        output.addGrouping(FILE_NAME, "access_log");
        output.addGrouping(FILE_LENGTH, 256);
        output.addValue(LABEL_COLUMN, COLUMN_A);
        output.addValue(LABEL_VALUE, "1");

        setFileName("access_log");
        setFileLength(256);
        run(LABELS, StringUtil.TAB, false, input, Arrays.asList(output));
    }

    @Override
    public Filter getFilter() {
        return new TestFilter();
    }
}
