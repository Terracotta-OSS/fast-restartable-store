/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.FileBuffer;
import java.io.IOException;

/**
 *
 * @author mscott
 */
public class MappedReadbackStrategyTest extends AbstractReadbackStrategyTest {
    
    public MappedReadbackStrategyTest() {
    }
    
    @Override
    public ReadbackStrategy getReadbackStrategy(Direction dir, FileBuffer buffer) throws IOException {
        return new MappedReadbackStrategy(buffer, dir);
    }
}
