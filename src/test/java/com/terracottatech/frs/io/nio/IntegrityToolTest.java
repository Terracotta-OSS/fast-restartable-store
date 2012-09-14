/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.log.LogRecordImpl;
import com.terracottatech.frs.log.LogRegionPacker;
import com.terracottatech.frs.log.Signature;
import com.terracottatech.frs.util.JUnitTestFolder;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author mscott
 */
public class IntegrityToolTest {
    File workArea;
    NIOManager manager; 
    long current;
    
    @Rule
    public JUnitTestFolder folder = new JUnitTestFolder(); 
    
    public IntegrityToolTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() throws Exception {
        workArea = folder.newFolder();
        System.out.println(workArea.getAbsolutePath());
        manager = new NIOManager(workArea.getAbsolutePath(), 1 * 1024 * 1024, 10 * 1024 * 1024);
        manager.setMinimumMarker(100);
    //  create a 10k lsn window
        for(int x=0;x<1000;x++) {
            writeChunkWithMarkers(10);
            manager.sync();
        }
        manager.close();
    }
    
    private void writeChunkWithMarkers(int size) throws Exception {
        ArrayList<LogRecord> list = new ArrayList<LogRecord>();
        list.add(new LogRecordImpl(new ByteBuffer[] {ByteBuffer.allocate(1024)}, null));
        manager.write(new LogRegionPacker(Signature.NONE).pack(list),current+=size);
    }
        
    
    @After
    public void tearDown() {
    }

    /**
     * Test of examine method, of class IntegrityTool.
     */
    @Test
    public void testBadDirectory() throws Exception {
        try {
            IntegrityTool tool = new IntegrityTool(folder.newFolder());
            tool.examine();
            Assert.fail("the directory is supposed to have segment files");
        } catch ( Exception exp ) {
            assert(exp instanceof IOException);
        }
        try {
            IntegrityTool tool = new IntegrityTool(folder.newFile());
            tool.examine();
            Assert.fail("the directory is not supposed to be a file");
        } catch ( Exception exp ) {
            assert(exp instanceof IOException);
        }        
        try {
            IntegrityTool tool = new IntegrityTool(new File("THISFILEDOESNOTEXIST"));
            tool.examine();
            Assert.fail("the directory is supposed to be missing");
        } catch ( Exception exp ) {
            assert(exp instanceof IOException);
        }  
        
//  this should work
        
        IntegrityTool tool = new IntegrityTool(workArea);
        tool.examine();
       
    }

    /**
     * Test of examineSegmentFile method, of class IntegrityTool.
     */
    @Test 
    public void testExamineSegmentFile() throws Exception {
        System.out.println("examineSegmentFile");
        File f = new NIOSegmentList(workArea).getBeginningFile();
        IntegrityTool tool = new IntegrityTool(workArea);
        assert(tool.examineSegmentFile(f) == 100);
    }
    
    @Test
    public void testMisAlignment() throws Exception {
        try {
            IntegrityTool tool = new IntegrityTool(workArea);
            tool.examineSegmentFile(folder.newFile());
            Assert.fail("file should not be a part of this stream");
        } catch ( Exception e ) {
            e.printStackTrace();
            assert( e instanceof IOException );
        }
    }
}
