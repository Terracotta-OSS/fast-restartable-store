/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.FileBuffer;
import com.terracottatech.frs.io.IOManager;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mscott
 */
public class IntegrityReadbackStrategy extends AbstractReadbackStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(IOManager.class);
    private FileBuffer buffer;
    private long lastGood = 0;
    private long lastMarker = 0;
    private boolean primed = false;
    private boolean done = false;
    private ArrayList<Long> jumpList = new ArrayList<Long>();
    private int exitStatus;
    

    public IntegrityReadbackStrategy(FileBuffer src) {
        buffer = src;
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
                jumpList.add(buffer.position());
            } else {
                throw new IOException(new String(check));
            }
        } catch (Exception ioe) {
            LOGGER.error("io error checking integrity",ioe);
            done = true;
        }
        primed = false;
        return null;
    }

    @Override
    public Iterator<Chunk> iterator() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    long getLastValidPosition() {
        return lastGood;
    }

    long getLastValidMarker() {
        return lastMarker;
    }
    
    List<Long> getJumpList() {
        return jumpList;
    }

    boolean wasClosed() {
        return SegmentHeaders.CLOSE_FILE.validate(exitStatus);
    }
}
