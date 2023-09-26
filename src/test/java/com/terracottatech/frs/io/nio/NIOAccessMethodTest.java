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

import java.util.Arrays;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author mscott
 */
public class NIOAccessMethodTest {
    
    public NIOAccessMethodTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of values method, of class NIOAccessMethod.
     */
    @Test
    public void testValues() {
        System.out.println("values");
        NIOAccessMethod[] result = NIOAccessMethod.values();
        assertTrue(Arrays.binarySearch(result, NIOAccessMethod.MAPPED) >= 0);
        assertTrue(Arrays.binarySearch(result, NIOAccessMethod.STREAM) >= 0);
    }

    /**
     * Test of valueOf method, of class NIOAccessMethod.
     */
    @Test
    public void testValueOf() {
        System.out.println("valueOf");
        assertTrue(NIOAccessMethod.valueOf("STREAM") == NIOAccessMethod.STREAM);
        assertTrue(NIOAccessMethod.valueOf("MAPPED") == NIOAccessMethod.MAPPED);
    }

    /**
     * Test of getDefault method, of class NIOAccessMethod.
     */
    @Test
    public void testGetDefault() {
        System.out.println("getDefault");
        NIOAccessMethod expResult = NIOAccessMethod.MAPPED;
        NIOAccessMethod result = NIOAccessMethod.getDefault();
        assertEquals(expResult, result);
    }
}