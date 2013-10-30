/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
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