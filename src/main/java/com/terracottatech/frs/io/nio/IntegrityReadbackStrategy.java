/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.FileBuffer;
import com.terracottatech.frs.util.ByteBufferUtils;
import java.io.EOFException;
import java.io.IOException;
import java.util.Iterator;

/**
 *
 * @author mscott
 */
public class IntegrityReadbackStrategy extends AbstractReadbackStrategy {

    private FileBuffer buffer;
    private long lastGood = 0;
    private long lastMarker = 0;
    private boolean primed = false;
    private boolean done = false;

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
        return !SegmentHeaders.CLOSE_FILE.validate(buffer.peekInt());
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
            } else {
                throw new IOException(new String(check));
            }
        } catch (Exception ioe) {
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

    boolean wasClosed() {
        return !SegmentHeaders.CLOSE_FILE.validate(buffer.peekInt());
    }
}
