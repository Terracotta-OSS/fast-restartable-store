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
