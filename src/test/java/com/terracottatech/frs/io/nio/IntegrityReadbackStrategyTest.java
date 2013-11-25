/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.FileBuffer;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author mscott
 */
public class IntegrityReadbackStrategyTest extends AbstractReadbackStrategyTest {

    public IntegrityReadbackStrategyTest() {
    }

    @Override
    public ReadbackStrategy getReadbackStrategy(Direction dir, FileBuffer buffer) throws IOException {
        IntegrityReadbackStrategy ir =  new IntegrityReadbackStrategy(buffer);
        while ( ir.hasMore(Direction.FORWARD) ) {
            ir.iterate(Direction.FORWARD);
        }
        ir.clear();
        buffer.position(0);
        return ir;
    }
    
    @Override @Test
    public void testIterate() throws Exception {
        FileBuffer buffer = new FileBuffer(new RandomAccessFile(folder.newFile(),"rw").getChannel(),ByteBuffer.allocate(8192));
        writeCompleteBuffer(buffer, "test scanning a complete buffer");
        ReadbackStrategy instance = getReadbackStrategy(Direction.FORWARD, buffer);
        assertNull(instance.iterate(Direction.FORWARD));
        boolean exception = false;
        try {
            instance.iterate(Direction.REVERSE);
        } catch ( IOException e ) {
            exception = true;
        }
        assertTrue(exception);
    }

    @Override @Test
    public void testScan() throws Exception {
        FileBuffer buffer = new FileBuffer(new RandomAccessFile(folder.newFile(),"rw").getChannel(),ByteBuffer.allocate(8192));
        writeCompleteBuffer(buffer, "test scanning a complete buffer");
        ReadbackStrategy instance = getReadbackStrategy(Direction.RANDOM, buffer);
        //  starts at 100 so 103 should be "complete"
        Chunk c = instance.scan(103L);
        assertNull(c);
    }

  @Override @Test 
  public void testLargeScan() throws Exception {
    //  not relevant
  }
    
    
    
}
