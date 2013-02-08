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
import org.huahinframework.core.io.Record;
import org.huahinframework.core.util.StringUtil;
import org.huahinframework.core.writer.Writer;
import org.junit.Test;

/**
 *
 */
public class JobDriverBigJoinSomeColumnTest extends JobDriver {
    private static final String LABEL_ID_1 = "ID1";
    private static final String LABEL_ID_2 = "ID2";
    private static final String LABEL_VALUE = "VALUE";
    private static final String LABEL_NAME = "NAME";

    private static final String[] LABELS = { LABEL_ID_1, LABEL_ID_2, LABEL_VALUE };
    private static final String[] MASTER_LABELS = { LABEL_ID_1, LABEL_ID_2, LABEL_NAME };

    public static class TestFilter extends Filter {
        @Override
        public void init() {
        }

        @Override
        public void filter(Record record, Writer writer)
                throws IOException, InterruptedException {
            Record emitRecord = new Record();
            emitRecord.addGrouping(LABEL_ID_1, record.getValueString(LABEL_ID_1));
            emitRecord.addGrouping(LABEL_ID_2, record.getValueString(LABEL_ID_2));
            emitRecord.addValue(LABEL_VALUE, record.getValueString(LABEL_VALUE));
            emitRecord.addValue(LABEL_NAME, record.getValueString(LABEL_NAME));
            writer.write(emitRecord);
        }

        @Override
        public void filterSetup() {
        }
    }

    @Test
    public void test()
            throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, URISyntaxException {
        addJob(LABELS, StringUtil.TAB).setFilter(TestFilter.class);

        List<String> input = new ArrayList<String>();
        input.add(1 + StringUtil.TAB + 1 + StringUtil.TAB + "A");
        input.add(2 + StringUtil.TAB + 2 + StringUtil.TAB + "B");
        input.add(3 + StringUtil.TAB + 3 + StringUtil.TAB + "C");
        input.add(4 + StringUtil.TAB + 4 + StringUtil.TAB + "D");
        input.add(5 + StringUtil.TAB + 5 + StringUtil.TAB + "E");

        List<String> masterData = new ArrayList<String>();
        masterData.add(1 + StringUtil.TAB + 1 + StringUtil.TAB + "A1");
        masterData.add(2 + StringUtil.TAB + 2 + StringUtil.TAB + "B2");
        masterData.add(3 + StringUtil.TAB + 3 + StringUtil.TAB + "C3");
        masterData.add(4 + StringUtil.TAB + 4 + StringUtil.TAB + "D4");
        masterData.add(5 + StringUtil.TAB + 5 + StringUtil.TAB + "E5");

        List<Record> output = new ArrayList<Record>();
        output.add(createMaster("A", 1));
        output.add(createMaster("B", 2));
        output.add(createMaster("C", 3));
        output.add(createMaster("D", 4));
        output.add(createMaster("E", 5));

        String[] jm = { LABEL_ID_1, LABEL_ID_2 };
        String[] jd = { LABEL_ID_1, LABEL_ID_2 };
        setBigJoin(MASTER_LABELS, jm, jd, masterData);

        run(input, output, true);
    }

    private Record createMaster(String s, int i) {
        Record r = new Record();
        r.addGrouping(LABEL_ID_1, String.valueOf(i));
        r.addGrouping(LABEL_ID_2, String.valueOf(i));
        r.addValue(LABEL_VALUE, s);
        r.addValue(LABEL_NAME, s + i);
        return r;
    }
}
