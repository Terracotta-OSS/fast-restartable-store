/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.FileBuffer;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 *
 * @author mscott
 */
public class MappedReadbackStrategyTest extends AbstractReadbackStrategyTest {
    
    public MappedReadbackStrategyTest() {
    }
    
    @Override
    public ReadbackStrategy getReadbackStrategy(Direction dir, FileBuffer buffer) throws IOException {
        return new MappedReadbackStrategy(buffer.getFileChannel(), dir);
    }
}
