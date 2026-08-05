/*
 * Copyright IBM Corp. 2024, 2025
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

import com.terracottatech.frs.Constants;
import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.FileBuffer;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.util.ByteBufferUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mscott
 */
public class NIOSegment {
    static final int FILE_HEADER_SIZE = 42;
    private static final short IMPL_NUMBER = 02;
    private final NIOStreamImpl parent;
    private final int segNum;
    private final File src;
    
    private long lowestMarker;
    private long minMarker;
    private boolean wasClosed;
    private long size;
    private UUID streamId;
    
    static final Logger LOGGER = LoggerFactory.getLogger(IOManager.class);

    NIOSegment(NIOStreamImpl p, File file) {
        this.parent = p;
        this.src = file;
        this.segNum = NIOConstants.convertSegmentNumber(file);
    }

    File getFile() {
        return src;
    }
    
    NIOStreamImpl getStream() {
        return parent;
    }
   
    private FileChannel createFileChannel() throws IOException {
        return new FileInputStream(getFile()).getChannel();
    }
    
    NIOSegment openForHeader() throws IOException, HeaderException {
        FileBuffer buffer = (parent != null ) ? 
                parent.createFileBuffer(createFileChannel(), FILE_HEADER_SIZE) :
                new FileBuffer(createFileChannel(), ByteBuffer.allocate(FILE_HEADER_SIZE));
        
        size = buffer.size();

        if (size < FILE_HEADER_SIZE) {
            buffer.close();
            throw new HeaderException("bad header", this);
        }
        
        buffer.read(1);
        readFileHeader(buffer);
        wasClosed = wasProperlyClosed(buffer);

        buffer.close();
        
        return this;
    }      
    
    
    void readFileHeader(Chunk readBuffer) throws IOException, HeaderException {
        if ( readBuffer.remaining() < FILE_HEADER_SIZE ) {
            throw new IOException("file buffering size too small");
        }
                
        byte[] code = new byte[4];
        if ( readBuffer.get(code) != 4 ) {
            throw new HeaderException("empty file", this);
        }
        if (!SegmentHeaders.LOG_FILE.validate(code)) {
            throw new HeaderException("file header is corrupted " + new String(code), this);
        }
        short impl = readBuffer.getShort();
        int checkSeg = readBuffer.getInt();
        if (segNum != checkSeg) {
            throw new HeaderException("the filename does not match the internal file structure", this);
        }

        if (impl != IMPL_NUMBER) {
            throw new HeaderException("unknown implementation number", this);
        }

        streamId = new UUID(readBuffer.getLong(), readBuffer.getLong());
        lowestMarker = readBuffer.getLong();
        minMarker = readBuffer.getLong();
    }
    
    void insertFileHeader(long lowestMarker, long marker) throws IOException {
        this.lowestMarker = lowestMarker;
        this.minMarker = marker;
        
        if ( lowestMarker < Constants.GENESIS_LSN || marker < Constants.GENESIS_LSN ) {
            throw new AssertionError("bad markers");
        }
 //  parent is null only in tests and read-only ops       
        this.streamId = ( parent != null ) ? parent.getStreamId() : UUID.randomUUID();
    }
    
    public int getSegmentId() {
        return segNum;
    }

    UUID getStreamId() {
        return streamId;
    }

    long getBaseMarker() {
        return minMarker;
    }

    long getMinimumMarker() {
        return lowestMarker;
    }
    
    long size() {
        return size;
    }
    
    public boolean wasProperlyClosed() {
        return wasClosed;
    }

    private boolean wasProperlyClosed(FileBuffer buffer) throws IOException {        
        buffer.clear();
        buffer.position(buffer.size() - buffer.capacity()).read(1);
        int fileEnd = buffer.getInt(buffer.remaining() - ByteBufferUtils.INT_SIZE);
        return (SegmentHeaders.CLOSE_FILE.validate(fileEnd) || SegmentHeaders.JUMP_LIST.validate(fileEnd));
    }

  @Override
  public String toString() {
    return "NIOSegment{" + "segNum=" + segNum + ", src=" + src + ", lowestMarker=" + lowestMarker + ", minMarker=" + minMarker + ", wasClosed=" + wasClosed + '}';
  }
}