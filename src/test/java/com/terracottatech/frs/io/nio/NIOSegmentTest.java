/*
 * Copyright (c) 2013-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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
