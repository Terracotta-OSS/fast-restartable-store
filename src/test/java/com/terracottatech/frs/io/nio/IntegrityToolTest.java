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
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.MaskingBufferSource;
import com.terracottatech.frs.io.SplittingBufferSource;
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

import static com.terracottatech.frs.config.FrsProperty.FORCE_LOG_REGION_FORMAT;

/**
 *
 * @author mscott
 */
public class IntegrityToolTest {
    File workArea;
    NIOManager manager; 
    long current = 100;
    private static BufferSource src;
    
    @Rule
    public JUnitTestFolder folder = new JUnitTestFolder(); 
    
    public IntegrityToolTest() {
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
        workArea = folder.newFolder();
        System.out.println(workArea.getAbsolutePath());
        manager = new NIOManager(workArea.getAbsolutePath(), NIOAccessMethod.NONE.toString(), null, 1 * 1024 * 1024, -1, -1, false, src);
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
        manager.write(new LogRegionPacker(Signature.NONE, (String) FORCE_LOG_REGION_FORMAT.defaultValue()).pack(list),current+=size);
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
