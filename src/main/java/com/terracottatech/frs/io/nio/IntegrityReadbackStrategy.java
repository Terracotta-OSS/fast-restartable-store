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
import com.terracottatech.frs.io.IOManager;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mscott
 */
public class IntegrityReadbackStrategy extends AbstractReadbackStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(IOManager.class);
    private final FileBuffer buffer;
    private final WritingSegmentJumpList jumpList = new WritingSegmentJumpList();
    private long lastGood = 0;
    private long lastMarker = 0;
    private boolean primed = false;
    private boolean done = false;
    private int exitStatus;
    

    public IntegrityReadbackStrategy(FileBuffer src) {
        buffer = src;
    }

    public FileBuffer getFileBuffer() {
        buffer.clear();
        return buffer;
    }

    @Override
    public long getMaximumMarker() {
        return lastMarker;
    }

    @Override
    public boolean hasMore(Direction dir) throws IOException {
        if (done) {
            return false;
        }
        if (buffer.size() - buffer.position() < 12) {
            return false;
        }
        prime();
        int check = buffer.peekInt();
        if ( SegmentHeaders.CLOSE_FILE.validate(check) ) {
            exitStatus = check;
            return false;
        }
        return true;
    }

    private void prime() throws IOException {
        if (primed) {
            return;
        }
        buffer.clear();
        buffer.partition(12);
        buffer.read(1);
        primed = true;
    }

    @Override
    public Chunk iterate(Direction dir) throws IOException {
        try {
            if (dir != Direction.FORWARD) {
                throw new IOException("only forward movement allowed");
            }
            prime();
            byte[] check = new byte[4];
            buffer.get(check);
            if (SegmentHeaders.CHUNK_START.validate(check)) {
                long size = buffer.getLong();
                long moveTo = buffer.position() + size;
                if (moveTo >= buffer.size() + 20) {
                    throw new EOFException();
                }
                buffer.clear();
                buffer.position(moveTo);
                buffer.partition(20);
                buffer.read(1);
                long clen = buffer.getLong();
                if (clen != size) {
                    throw new IOException("inconsistent");
                }
                lastMarker = buffer.getLong();
                if (SegmentHeaders.FILE_CHUNK.validate(buffer.getInt())) {
                    lastGood = buffer.position();
                }
                jumpList.add(lastGood);
            } else {
                throw new IOException(new String(check));
            }
        } catch (Exception ioe) {
            logPosition(ioe);
            done = true;
            if ( ioe instanceof IOException ) {
                throw (IOException)ioe;
            } else {
                throw new IOException(ioe);
            }
        }
        primed = false;
        return null;
    }
    
    private void logPosition(Exception e) {
        if ( LOGGER.isDebugEnabled() ) {
            try {
            LOGGER.debug(new Formatter(new StringBuilder()).format("io error checking integrity file: %s last valid pos: %d current pos: %d last valid marker: %d",buffer.toString(),
                    this.getLastValidPosition(),this.buffer.position(),this.getMaximumMarker()).out().toString(),e);
            } catch ( Throwable t ) {
                LOGGER.debug("unexpected",t);
            }
        }
    }

    long getLastValidPosition() {
        return lastGood;
    }
    
    WritingSegmentJumpList getJumpList() {
        return jumpList;
    }

    void clear() {
        buffer.clear();
        primed = false;
    }

    boolean wasClosed() {
        return SegmentHeaders.CLOSE_FILE.validate(exitStatus);
    }

    @Override
    public long size() throws IOException {
        return buffer.size();
    }

    @Override
    public Chunk scan(long marker) throws IOException {
        return null;
    }

    @Override
    public boolean isConsistent() {
        return SegmentHeaders.CLOSE_FILE.validate(exitStatus);
    }
    
    
}
