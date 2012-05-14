/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.nio.NIOManager;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

/**
 *
 * @author mscott
 */
public class LogManagerThroughputTest {

    private static final long MAX_SEGMENT_SIZE = 10 * 1024 * 1024;
    NIOManager stream;
    LogManager mgr;
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public LogManagerThroughputTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        stream = new NIOManager(folder.getRoot().getAbsolutePath(), MAX_SEGMENT_SIZE);
        mgr = new StagingLogManager(Signature.ADLER32, new AtomicCommitList( 100l, 64, 20),stream);
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
    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    // @Test
    // public void hello() {}
}
