/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.Exchanger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private long lowestMarkerOnDisk = 0;
    private long highestMarker = 0;
    private long currentMarker = 0;
    private NIOSegmentImpl writeHead;
    private NIOSegmentImpl readHead;
    private ManualBufferSource manualPool;
    private RotatingBufferSource gcPool;
    private FSyncer syncer;
    private static final Logger LOGGER = LoggerFactory.getLogger(IOManager.class);
    private BufferBuilder createBuffer;
//    private long debugIn;
//    private long debugOut;
    public NIOStreamImpl(File filePath, long recommendedSize) throws IOException {
        this(filePath,recommendedSize, recommendedSize);
    }
    
    public NIOStreamImpl(File filePath, long recommendedSize, long memorySize) throws IOException {
        directory = filePath;

        segmentSize = recommendedSize;
        if ( memorySize < segmentSize * 2 ) {
            memorySize = segmentSize * 2;
        }
        
        manualPool = new ManualBufferSource(memorySize);
        gcPool = new RotatingBufferSource(manualPool);
//        gcPool.setNoFail();
        segments = new NIOSegmentList(directory);

        if (segments.isEmpty()) {
            streamId = UUID.randomUUID();
        } else {
            try {
                NIOSegmentImpl seg = new NIOSegmentImpl(this, segments.getBeginningFile());
                seg.openForHeader(manualPool);
                streamId = seg.getStreamId();
                seg.close();
            } catch ( HeaderException header ) {
                streamId = UUID.randomUUID();
            } catch ( IOException ioe ) {
                streamId = UUID.randomUUID();
            }
        }
    }
    
    public void setBufferBuilder(BufferBuilder builder) {
        createBuffer = builder;
    }
    
    BufferBuilder getBufferBuilder() {
        return createBuffer;
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
        gcPool.reclaim();
        return currentMarker;
    }

    public long getMinimumMarker() {
        gcPool.reclaim();
        return lowestMarker;
    }

    public long getMaximumMarker() {
        gcPool.reclaim();
        return highestMarker;
    }

    public void setMaximumMarker(long marker) {
        this.highestMarker = marker;
    }

    boolean checkForCleanExit() throws IOException {
        if (segments.isEmpty()) {
            return true;
        }
        NIOSegmentImpl seg = new NIOSegmentImpl(this, segments.getEndFile());
        try {
            try {
                seg.openForHeader(manualPool);
            } catch ( HeaderException header ) {
                return false;
            }
            if (!seg.getStreamId().equals(streamId)) {
                throw new IOException(BAD_STREAM_ID);
            }
            return seg.wasProperlyClosed();
        } finally {
            seg.close();
        }
    }

    boolean open() throws IOException {
/* TODO:   disable fsyncer for now.  validate for speed and accuracy
        syncer = new FSyncer();
        syncer.start();
*/
        if (segments.isEmpty()) {
            return false;
        }
        segments.setReadPosition(-1);
        boolean goodClose = false;
        while (!goodClose) {
            File f = segments.nextReadFile(Direction.REVERSE);
            if (f == null) {
                segments.removeFilesFromHead();
                return false;
            }
            NIOSegmentImpl seg = new NIOSegmentImpl(this, f);
            try {
                seg.openForHeader(manualPool);
                if (!seg.getStreamId().equals(streamId)) {
                    throw new IOException(BAD_STREAM_ID);
                }

                if (!seg.last()) {
                    //  segment did not close cleanly, mark for truncation at last good chunk
                    if (seg.isEmpty()) {
                        continue;
                    } else {
//   truncate and exit            
                        segments.removeFilesFromHead();
                        seg.limit(seg.position());
                        return true;
                    }
                } else {
                    segments.removeFilesFromHead();
                    return false;
                }
            } catch (HeaderException ioe) {
            //  bad header info in this file, move on.
            } finally {
                this.highestMarker = seg.getMaximumMarker();
                this.lowestMarker = seg.getMinimumMarker();
                this.lowestMarkerOnDisk = seg.getMinimumMarker();
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
                seg.openForHeader(manualPool);
                if (!seg.getStreamId().equals(streamId)) {
                    throw new IOException(BAD_STREAM_ID);
                }
                if (seg.getSegmentId() == segment) {
                    segments.removeFilesFromHead();
                    seg.limit(position);
                    return;
                }
            } catch ( HeaderException ioe ) {
            //  something is wrong with this file move on.
            } finally {
                seg.close();
            }
            f = segments.nextReadFile(Direction.REVERSE);
        }
    }

//  make sure this segment backets lowest marker
    private boolean doubleCheck(File f) throws IOException {
        NIOSegmentImpl segment = new NIOSegmentImpl(this, f);
        try {
            segment.openForHeader(manualPool);
        } catch ( HeaderException header ) {
//  this should not happen here
            throw new IOException(header);
        }

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
    
    long findLogTail() throws IOException {
        segments.setReadPosition(0);
        File f = segments.nextReadFile(Direction.FORWARD);
        long size = 0;
        while (f != null) {

            NIOSegmentImpl seg = new NIOSegmentImpl(this, f);
            try {
                try {
                    seg.openForHeader(manualPool);
                } catch ( HeaderException header ) {
// this should not happen here, rethrow as a IOException
                    throw new IOException(header);
                }
                if (!seg.getStreamId().equals(streamId)) {
                    throw new IOException(BAD_STREAM_ID);
                }
                
                //  if the base is greater short circuit out
                if (seg.getBaseMarker() > this.lowestMarkerOnDisk) {
                    File last = segments.nextReadFile(Direction.REVERSE);
                    if ( last != null ) size -= last.length();
                    else size = 0;
                    return size;
                }
                
                // ran into the head.  we are done.
                if ( f.equals(segments.getEndFile()) ) {
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

    long trimLogTail(long timeout) throws IOException {
        if ( findLogTail() != 0 ) { // position the read had over the last dead segment
            File last = segments.getCurrentReadFile();
            assert(last!=null);
            if ( doubleCheck(last) ) {  //  make sure this is the right file, assert?!
                return segments.removeFilesFromTail();
            } else {
                
            }
        }
        return 0;
    }

    @Override
    public long append(Chunk c) throws IOException {

        if (writeHead == null || writeHead.isClosed()) {
            File f = segments.appendFile();
            
            gcPool.reclaim();
            writeHead = new NIOSegmentImpl(this, f).openForWriting(manualPool);
            writeHead.insertFileHeader(lowestMarker, currentMarker);
            lowestMarkerOnDisk = lowestMarker;
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
            try {
                writeHead.prepareForClose();
            } catch ( IOException ioe ) {
                //  silently fail, closing anyways
            }
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
        if ( LOGGER.isDebugEnabled() ) {
            LOGGER.debug(manualPool.toString());
        }
        
        gcPool.reclaim();
        manualPool.reclaim();
        
    }

    @Override
    public Chunk read(final Direction dir) throws IOException {

        while (readHead == null || !readHead.hasMore(dir)) {
            if (readHead != null) {
                readHead.close();
            }

            try {
                File f = segments.nextReadFile(dir);
                if (f == null) {
                    readHead = null;
                    return null;
                }
                NIOSegmentImpl nextHead = new NIOSegmentImpl(this, f).openForReading(gcPool);
                if ( readHead != null ) {
                    assert(nextHead.getSegmentId() - readHead.getSegmentId() + ((dir == Direction.FORWARD) ? -1 : +1) == 0);
                }
                readHead = nextHead;
//  files should be clean, any error needs to be thrown
            } catch (HeaderException header) {
                throw new IOException(header);
            } catch (IOException ioe) {
                throw ioe;
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
                    if ( next == null ) {
                        gcPool.reclaim();
                    }
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
