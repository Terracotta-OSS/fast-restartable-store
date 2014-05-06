/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.util.JUnitTestFolder;

import java.io.File;
import java.io.IOException;
import org.junit.*;


/**
 *
 * @author mscott
 */
public abstract class NIOSegmentTest {
    
    private File workarea;
       
    @Rule
    public JUnitTestFolder folder = new JUnitTestFolder();

    public NIOSegmentTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() throws IOException {
      workarea = folder.newFolder();
    }

    @After
    public void tearDown() {
    }
    
    public File getDirectory() {
      return workarea;
    }
}
