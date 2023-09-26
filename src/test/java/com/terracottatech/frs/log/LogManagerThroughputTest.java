/*
 * Copyright (c) 2012-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.Constants;
import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.MaskingBufferSource;
import com.terracottatech.frs.io.SplittingBufferSource;
import com.terracottatech.frs.io.nio.NIOAccessMethod;
import com.terracottatech.frs.io.nio.NIOManager;
import com.terracottatech.frs.util.JUnitTestFolder;

import org.junit.*;

/**
 *
 * @author mscott
 */
public class LogManagerThroughputTest {

    private static final long MAX_SEGMENT_SIZE = 10 * 1024 * 1024;
    NIOManager stream;
    LogManager mgr;
    private static BufferSource src;

    @Rule
    public JUnitTestFolder folder = new JUnitTestFolder();

    public LogManagerThroughputTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
      src = new MaskingBufferSource(new SplittingBufferSource(16, 8 * 1024 * 1024));
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        stream = new NIOManager(folder.getRoot().getAbsolutePath(), NIOAccessMethod.NONE.toString(),null,  MAX_SEGMENT_SIZE, -1, -1, false, src);
        mgr = new StagingLogManager(Signature.ADLER32, new AtomicCommitList( Constants.FIRST_LSN, 64, 20),stream, src);
        mgr.startup();
    }
    
    @Test
    public void testTP() throws Exception {
        long count = 0;
        int it = 0;
        long total = System.nanoTime();
        while ( count < 1l * 1024 * 1024 * 1024 ) {
            DummyLogRecord log = new DummyLogRecord(1024,10 * 1024);
            count += log.size();
            if ( it++ % 100 == 99 ) {
                mgr.appendAndSync(log).get();
            } else {
                mgr.append(log);
            }
        }
        System.out.format("%.3f  MB/s\n",(count/(1024*1024))/((System.nanoTime()-total)/1e9));
    }

    @After
    public void tearDown() {
        mgr.shutdown();
    }
}
