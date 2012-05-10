/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.Exchanger;

/**
 * NIO implementation of Log Stream.
 *
 * @author mscott
 */
class NIOStreamImpl implements Stream {

    private static final String BAD_STREAM_ID = "mis-aligned streams";
    private final File directory;
    private final long segmentSize;
    private NIOSegmentList segments;
    private UUID streamId;
    private long lowestMarker = 0;
    private long highestMarker = 0;
    private long currentMarker = 0;
    private NIOSegmentImpl writeHead;
    private NIOSegmentImpl readHead;
    private final RotatingBufferSource pool = new RotatingBufferSource();
    private FSyncer syncer;
    private long debugIn;
    private long debugOut;

    public NIOStreamImpl(File filePath, long recommendedSize) throws IOException {
        directory = filePath;

        segmentSize = recommendedSize;

        segments = new NIOSegmentList(directory);

        if (segments.isEmpty()) {
            streamId = UUID.randomUUID();
        } else {
            NIOSegmentImpl seg = new NIOSegmentImpl(this, segments.getEndFile());
            seg.openForReading(pool);
            streamId = seg.getStreamId();
            seg.close();
        }
    }

    @Override
    public UUID getStreamId() {
        return streamId;
    }

    public void setMinimumMarker(long lowestMarker) {
        this.lowestMarker = lowestMarker;
    }

    public void setMarker(long marker) {
        currentMarker = marker;
    }

    public long getMarker() {
        return currentMarker;
    }

    public long getMinimumMarker() {
        return lowestMarker;
    }

    public void setMaximumMarker(long marker) {
        this.highestMarker = marker;
    }

    public long getMaximumMarker() {
        return highestMarker;
    }

    boolean checkForCleanExit() throws IOException {
        if (segments.isEmpty()) {
            return true;
        }
        NIOSegmentImpl seg = new NIOSegmentImpl(this, segments.getEndFile());
        try {
            pool.reclaim();
            seg.openForReading(pool);
            if (!seg.getStreamId().equals(streamId)) {
                throw new IOException(BAD_STREAM_ID);
            }
            return seg.wasProperlyClosed();
        } finally {
            seg.close();
        }
    }

    boolean open() throws IOException {
        syncer = new FSyncer();
        syncer.start();

        if (segments.isEmpty()) {
            return false;
        }
        segments.setReadPosition(-1);
        boolean goodClose = false;
        while (!goodClose) {
            File f = segments.nextReadFile(Direction.REVERSE);
            if (f == null) {
                return false;
            }
            NIOSegmentImpl seg = new NIOSegmentImpl(this, f);
            try {
                pool.reclaim();
                seg.openForReading(pool);
                if (!seg.getStreamId().equals(streamId)) {
                    throw new IOException(BAD_STREAM_ID);
                }

                if (!seg.last()) {
                    //  segment did not close cleanly, mark for truncation at last good chunk
                    if (seg.isEmpty()) {
                        segments.removeCurrentSegment();
                        continue;
                    } else {
//   truncate and exit
                        seg.limit(seg.position());
                        seg.close();
                        return true;
                    }
                } else {
                    return false;
                }
            } finally {
                this.highestMarker = seg.getMaximumMarker();
                this.lowestMarker = seg.getMinimumMarker();
                seg.close();
            }
        }

        return true;
    }

    void limit(UUID streamId, int segment, long position) throws IOException {
        segments.setReadPosition(-1);
        File f = segments.nextReadFile(Direction.REVERSE);
        while (f != null) {
            NIOSegmentImpl seg = new NIOSegmentImpl(this, f);
            try {
                pool.reclaim();
                seg.openForReading(pool);
                if (!seg.getStreamId().equals(streamId)) {
                    throw new IOException(BAD_STREAM_ID);
                }
                if (seg.getSegmentId() > segment) {
                    segments.removeCurrentSegment();
                }
                if (seg.getSegmentId() == segment) {
                    seg.limit(position);
                    return;
                }
            } finally {
                seg.close();
            }
            f = segments.nextReadFile(Direction.REVERSE);
        }
    }

//  make sure this segment backets lowest marker
    private boolean doubleCheck(File f) throws IOException {
        NIOSegmentImpl segment = new NIOSegmentImpl(this, f);
        segment.openForReading(pool);

        if (segment.getBaseMarker() > this.lowestMarker) {
            return false;
        }
//   gotta scan it
        if (!segment.last()) {
//  not stable and closed, just exit
            return false;
        }
        if (segment.getMaximumMarker() > this.lowestMarker) {
            return true;
        } else {
            return false;
        }
    }  
    
    long findLogHead() throws IOException {
        segments.setReadPosition(0);
        File f = segments.nextReadFile(Direction.FORWARD);
        long size = 0;
        while (f != null) {
            NIOSegmentImpl seg = new NIOSegmentImpl(this, f);
            try {
                pool.reclaim();
                seg.openForReading(pool);
                if (!seg.getStreamId().equals(streamId)) {
                    throw new IOException(BAD_STREAM_ID);
                }
                //  if the base is greater short circuit out
                if (seg.getBaseMarker() > this.lowestMarker) {
                    File last = segments.nextReadFile(Direction.REVERSE);
                    size -= last.length();
                    return size;
                }
                size += seg.length();
            } finally {
                seg.close();
            }
            f = segments.nextReadFile(Direction.FORWARD);
        }
        return size;
    }

    long trimLogHead(long timeout) throws IOException {
        findLogHead(); // position the read had over the last dead segment
        File last = segments.getCurrentReadFile();
        if ( doubleCheck(last) ) {  //  make sure this is the right file, assert?!
            return segments.removeFilesFromHead();
        }
        return 0;
    }

    @Override
    public long append(Chunk c) throws IOException {

        if (writeHead == null || writeHead.isClosed()) {
            File f = segments.appendFile();

            pool.reclaim();
            writeHead = new NIOSegmentImpl(this, f).openForWriting(pool);
            writeHead.insertFileHeader(lowestMarker, currentMarker);
        }

        long w = writeHead.append(c, this.highestMarker);
        if (writeHead.length() > segmentSize) {
            writeHead.prepareForClose();
//            debugIn += writeHead.close();
            if ( syncer != null ) syncer.pivot(writeHead);
            else writeHead.close();
        }
        return w;

    }

    static class FSyncer extends Thread {

        private Exchanger<NIOSegmentImpl> pivot = new Exchanger<NIOSegmentImpl>();

        public FSyncer() {
            setName("fsync helper");
            setDaemon(true);
        }

        NIOSegmentImpl pivot(NIOSegmentImpl target) {
            try {
                return pivot.exchange(target);
            } catch (InterruptedException ie) {
            } finally {
            }
            return null;
        }

        public void run() {
            try {
                NIOSegmentImpl seg = null;
                seg = pivot.exchange(seg);
                while (!Thread.interrupted()) {
                    if ( seg == null ) {
                        
                    } else if (seg.isClosed()) {
                        seg.close();
                    } else {
                        seg.fsync();
                    }
                    seg = pivot.exchange(seg);
                }
            } catch (InterruptedException ie) {
            } catch (IOException ioe) {
            } finally {
            }
        }
    }
    //  fsync current segment.  old segments are fsyncd on close

    @Override
    public long sync() throws IOException {
        if (writeHead != null && !writeHead.isClosed()) {
            if (syncer != null) {
                long pos = writeHead.position();
                syncer.pivot(writeHead);
            //  get it back
                NIOSegmentImpl check = syncer.pivot(null);
                assert(check == writeHead);
                return pos;
            } else {
                return writeHead.fsync();
            }
        }
        return -1;
    }
    //  segment implementation forces before close.  neccessary?

    @Override
    public void close() throws IOException {
        if (writeHead != null && !writeHead.isClosed()) {
            writeHead.close();
        }
        writeHead = null;
        if (readHead != null && !readHead.isClosed()) {
            readHead.close();
        }
        readHead = null;

        if (syncer != null) {
            syncer.interrupt();
            try {
                syncer.join();
            } catch ( InterruptedException ie ) {
                throw new IOException(ie);
            }
        } 
        System.out.println("buffer pool created: " + pool.getCount() + " released: " + pool.getReleased() + " capacity: " + pool.getCapacity());
    }

    @Override
    public Chunk read(final Direction dir) throws IOException {

        while (readHead == null || !readHead.hasMore(dir)) {
            if (readHead != null) {
                debugOut += readHead.close();
            }
            pool.reclaim();

            try {
                File f = segments.nextReadFile(dir);
                if (f == null) {
                    return null;
                }
                readHead = new NIOSegmentImpl(this, f).openForReading(pool);
            } catch (IOException ioe) {
                if (readHead == null && dir == Direction.REVERSE) {
//  something bad happened.  Can't even open the header.  but this is the end of the log so move on to the next and see if it will open
                } else {
                    throw ioe;
                }
            }

            checkStreamId(readHead);
        }

        return readHead.next(dir);
    }

    private void checkStreamId(NIOSegmentImpl segment) throws IOException {
        if (!streamId.equals(segment.getStreamId())) {
            throw new IOException(BAD_STREAM_ID);
        }
    }

    @Override
    public void seek(long loc) throws IOException {
        segments.setReadPosition(loc);
        if (readHead != null) {
            readHead.close();
        }
        readHead = null;
    }

    int getSegmentId() {
        return readHead.getSegmentId();
    }
    
    long getTotalSize() {
        return segments.getTotalSize();
    }

    @Override
    public Iterator<Chunk> iterator() {
        return new Iterator<Chunk>() {

            Chunk next;

            @Override
            public boolean hasNext() {
                try {
                    if (next != null) {
                        return true;
                    }
                    next = read(Direction.getDefault());
                    return (next != null);
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            }

            @Override
            public Chunk next() {
                if (!hasNext()) {
                    throw new IndexOutOfBoundsException();
                }
                return next;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        };
    }
}
