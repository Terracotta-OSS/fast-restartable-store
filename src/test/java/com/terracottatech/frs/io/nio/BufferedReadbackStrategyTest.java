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

import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.FileBuffer;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.junit.Test;
import org.mockito.Mockito;

/**
 *
 * @author mscott
 */
public class BufferedReadbackStrategyTest extends AbstractReadbackStrategyTest {

    
    public BufferedReadbackStrategyTest() {
    }

    @Override
    public ReadbackStrategy getReadbackStrategy(Direction dir, FileBuffer buffer) throws IOException {
        return new BufferedReadbackStrategy(dir, buffer.getFileChannel(), null);
    }
    /**
     * Test of close method, of class BufferedRandomAccessStrategy.
     */
    @Test
    public void testClose() throws Exception {
        FileBuffer buffer = new FileBuffer(new RandomAccessFile(folder.newFile(),"rw").getChannel(),ByteBuffer.allocate(8192));
        FileChannel channel = Mockito.spy(buffer.getFileChannel());
        writeCompleteBuffer(buffer, "test close by making sure buffer.close() gets called");
        BufferedReadbackStrategy instance = new BufferedReadbackStrategy(Direction.RANDOM, channel, null);
        //  starts at 100 so 104 should be "complete"
        instance.close();
        Mockito.verify(channel).close();
    }

  @Override @Test
  public void testIsConsistent2() throws Exception {
    super.testIsConsistent2(); //To change body of generated methods, choose Tools | Templates.
  }
    
    
}