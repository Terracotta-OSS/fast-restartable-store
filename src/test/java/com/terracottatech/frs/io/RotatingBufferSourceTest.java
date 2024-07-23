/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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
package com.terracottatech.frs.io;

import org.junit.*;

/**
 *
 * @author mscott
 */
public class RotatingBufferSourceTest {
    
    RotatingBufferSource rotate = new RotatingBufferSource(new DirectBufferSource(20 * 1024 * 1024));
    
    public RotatingBufferSourceTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of getBuffer method, of class RotatingBufferSource.
     */
    @Test
    public void testGetBuffer() throws Exception {
        int size = 1024;
        while ( size < 10 *1024 *1024 ) {
            rotate.getBuffer(size*=2);
        }
        System.gc();
        rotate.spinsToFail(-1);
        rotate.millisToWait(5000);
        while ( size > 1024 ) {
            assert(rotate.getBuffer(size/=2) != null);
            Thread.sleep(100);
        }
    }


}
