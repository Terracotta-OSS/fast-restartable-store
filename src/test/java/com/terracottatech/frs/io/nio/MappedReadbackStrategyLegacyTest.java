/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.frs.io.nio;

import org.junit.Before;

/**
 *
 * @author mscott
 */
public class MappedReadbackStrategyLegacyTest extends AbstractReadbackStrategyLegacyTest {
    
    public MappedReadbackStrategyLegacyTest() {
    }

    @Before
    public void setup() throws Exception {
        super.setUp(NIOAccessMethod.MAPPED);
    }
}
