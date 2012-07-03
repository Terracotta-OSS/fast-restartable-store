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
    private final NIOSegmentList segments;
    private UUID streamId;
    private volatile long lowestMarker = 99;
    private volatile long lowestMarkerOnDisk = 0;
    private long currentMarker = 99;  //  init lsn is 100
    private NIOSegmentImpl writeHead;
    private NIOSegmentImpl readHead;
    private BufferSource manualPool;
    private FSyncer syncer;
    private static final Logger LOGGER = LoggerFactory.getLogger(IOManager.class);
    private BufferBuilder createBuffer;
    private HashMap<String, Integer> strategies;
    
    public NIOStreamImpl(File filePath, long recommendedSize) throws IOException {
        this(filePath,recommendedSize, recommendedSize);
    }
    
    public NIOStreamImpl(File filePath, long recommendedSize, long memorySize) throws IOException {
        directory = filePath;

        if ( LOGGER.isDebugEnabled() ) {
            strategies = new HashMap<String, Integer>();
        }
        segmentSize = recommendedSize;
        if ( memorySize < segmentSize * 4 ) {
            memorySize = segmentSize * 4;
        }
        LOGGER.debug("==CONFIG(nio)==" + filePath.getAbsolutePath() + " using a segment size of " + (segmentSize / (1024*1024)));
        LOGGER.debug("==CONFIG(nio)==" + filePath.getAbsolutePath() + " using a memory size of " + (memorySize / (1024*1024)));
        
        manualPool = new ManualBufferSource(new CachingBufferSource(), memorySize);

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

    public long getMarker() {
        return currentMarker;
    }

    public long getMinimumMarker() {
        return lowestMarker;
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
        
        ListIterator<File> files = segments.listIterator(segments.size());
        while ( files.hasPrevious() ) {
            File f = files.previous();
            NIOSegmentImpl seg = new NIOSegmentImpl(this, f);
            try {
                seg.openForHeader(manualPool);
                if (!seg.getStreamId().equals(streamId)) {
        //  fail,  the stream id does not equal segment's stream id
                    throw new IOException(BAD_STREAM_ID);
                }
                if ( seg.last() ) {
                    this.currentMarker = seg.getMaximumMarker();
                    this.lowestMarker = seg.getMinimumMarker();
                    this.lowestMarkerOnDisk = this.lowestMarker;
                    return true;                    
                }
            } catch ( HeaderException h ) {
                
            } finally {
                seg.close();
            }
   //  if here, the segment was bad, delete it
            files.remove();
            if ( f.exists() ) throw new IOException("unable to make log stream consistent");
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
                    if ( !segments.currentIsHead() ) {
                        throw new IOException("unable to make log stream consistent");
                    }
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

        if (segment.getBaseMarker() > this.lowestMarkerOnDisk) {
            return false;
        }
//   gotta scan it
        if (!segment.last()) {
//  not stable and closed, just exit
            return false;
        }
        if (segment.getMaximumMarker() > this.lowestMarkerOnDisk) {
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
            }
        }
        return 0;
    }

    @Override
    public long append(Chunk c, long marker) throws IOException {

        if (writeHead == null || writeHead.isClosed()) {
            File f = segments.appendFile();
            
            writeHead = new NIOSegmentImpl(this, f).openForWriting(manualPool);
            writeHead.insertFileHeader(lowestMarker, currentMarker+1);
        }

        long w = writeHead.append(c, marker);
        currentMarker = marker;
        if (writeHead.length() > segmentSize) {
            writeHead.prepareForClose();
//            debugIn += writeHead.close();
            if ( syncer != null ) syncer.pivot(writeHead);
            else writeHead.close();
        }
        return w;

    }

    class FSyncer extends Thread {

        private Exchanger<NIOSegmentImpl> pivot = new Exchanger<NIOSegmentImpl>();

        public FSyncer() {
            setName("fsync helper");
            setDaemon(true);
        }

        NIOSegmentImpl pivot(NIOSegmentImpl target) {
            try {
                return pivot.exchange(target);
            } catch (InterruptedException ie) {
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
                    lowestMarkerOnDisk = seg.getMinimumMarker();
                    seg = pivot.exchange(seg);
                }
            } catch (InterruptedException ie) {
            } catch (IOException ioe) {
            } 
        }
    }
    //  fsync current segment.  old segments are fsyncd on close

    @Override
    public long sync() throws IOException {
        if (writeHead != null && !writeHead.isClosed()) {
            long pos = writeHead.position();
            if (syncer != null) {
                syncer.pivot(writeHead);
            //  get it back
                NIOSegmentImpl check = syncer.pivot(null);
                assert(check == writeHead);
                return pos;
            } else {
                pos = writeHead.fsync();
                this.lowestMarkerOnDisk = writeHead.getMinimumMarker();
            }
            return pos;
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
            LOGGER.debug("==PERFORMANCE(memory)==" + manualPool.toString());
            StringBuilder slist = new StringBuilder();
            for ( Map.Entry<String,Integer> e : strategies.entrySet() ) {
                slist.append(" ");
                slist.append(e.getKey());
                slist.append(":");
                slist.append(e.getValue());
            }
            LOGGER.debug("==PERFORMANCE(strategies)==" + slist.toString());
        }
       
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
                NIOSegmentImpl nextHead = new NIOSegmentImpl(this, f).openForReading(manualPool);
                if ( readHead != null ) {
                    int expected = readHead.getSegmentId() + ((dir == Direction.REVERSE) ? -1 : +1);
                    if (nextHead.getSegmentId() != expected) {
                        throw new IOException("broken stream during readback");
                    }
                }
                if ( LOGGER.isDebugEnabled() ) {
                    String strat = nextHead.getStrategyDebug();
                    Integer count = strategies.get(strat);
                    if ( count == null ) {
                        strategies.put(strat,1);
                    } else {
                        strategies.put(strat,count+1);
                    }
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
    
    int getSegmentCount() {
        return segments.getCount();
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
