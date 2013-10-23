/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.terracottatech.frs.io.nio;

import org.junit.Before;

/**
 *
 * @author mscott
 */
public class NIOStreamMarkersTest extends NIOMarkersTest {
    
    public NIOStreamMarkersTest() {
    }
    
    
    @Before
    public void setUp() throws Exception {
        super.setUp(NIOAccessMethod.STREAM);
    }
}
