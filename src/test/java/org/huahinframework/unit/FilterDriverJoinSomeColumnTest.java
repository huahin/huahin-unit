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
import java.util.Arrays;
import java.util.List;

import org.huahinframework.core.Filter;
import org.huahinframework.core.io.Record;
import org.huahinframework.core.util.StringUtil;
import org.huahinframework.core.writer.Writer;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class FilterDriverJoinSomeColumnTest extends FilterDriver {
    private static final String LABEL_ID_1 = "ID1";
    private static final String LABEL_ID_2 = "ID2";
    private static final String LABEL_VALUE = "VALUE";
    private static final String LABEL_NAME = "NAME";

    private static final String[] LABELS = { LABEL_ID_1, LABEL_ID_2, LABEL_VALUE };
    private static final String[] MASTER_LABELS = { LABEL_ID_1, LABEL_ID_2, LABEL_NAME };

    private List<String> masterData;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        masterData = createMaster();
    }

    private static class TestFilter extends Filter {
        @Override
        public void filter(Record record, Writer writer)
                throws IOException, InterruptedException {
            String name = record.getValueString(LABEL_NAME);
            if (name == null) {
                return;
            }

            Record emitRecord = new Record();
            emitRecord.addGrouping(LABEL_NAME, name);
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
    public void testFirstHit() throws IOException, URISyntaxException {
        String input = "1" + StringUtil.TAB + "1" + StringUtil.TAB + "A";

        Record output = new Record();
        output.addGrouping(LABEL_NAME, "NAME_1");
        output.addValue(LABEL_VALUE, "A");

        String[] jm = { LABEL_ID_1, LABEL_ID_2 };
        String[] jd = { LABEL_ID_1, LABEL_ID_2 };
        setSimpleJoin(MASTER_LABELS, jm, jd, masterData);

        run(LABELS, StringUtil.TAB, false, input, Arrays.asList(output));
    }

    @Test
    public void testFirstNotHit() throws IOException, URISyntaxException {
        String input = "11" + StringUtil.TAB + "11" + StringUtil.TAB + "A";

        String[] jm = { LABEL_ID_1, LABEL_ID_2 };
        String[] jd = { LABEL_ID_1, LABEL_ID_2 };
        setSimpleJoin(MASTER_LABELS, jm, jd, masterData);

        run(LABELS, StringUtil.TAB, false, input, null);
    }

    private List<String> createMaster() {
        List<String> l = new ArrayList<String>();
        for (int i = 1; i <= 10; i++) {
            l.add(i + StringUtil.TAB + i + StringUtil.TAB + "NAME_" + i);
        }
        return l;
    }

    @Override
    public Filter getFilter() {
        return new TestFilter();
    }
}
