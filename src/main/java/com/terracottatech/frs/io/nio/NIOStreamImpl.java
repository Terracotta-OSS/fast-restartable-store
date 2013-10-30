/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.SnapshotRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.terracottatech.frs.io.BufferBuilder;
import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.FileBuffer;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.MaskingBufferSource;
import com.terracottatech.frs.io.SplittingBufferSource;
import com.terracottatech.frs.io.Stream;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Exchanger;

/**
 * NIO implementation of Log Stream.
 *
 * @author mscott
 */
class NIOStreamImpl implements Stream {
/*  concurrency needed for cloning */
    private final NIOSegmentList          segments;
    private volatile NIORandomAccess      randomAccess;

    static final String BAD_STREAM_ID = "mis-aligned streams";
    private final File directory;
    private final long segmentSize;

    private UUID streamId;
    private volatile long lowestMarker = 99;
    private volatile long lowestMarkerOnDisk = 0;
    private volatile long fsyncdMarker = 0;
    private long currentMarker = 99;  //  init lsn is 100
    private int  markerWaiters = 0;
    private WritingSegment writeHead;
    private ReadOnlySegment readHead;
    private long offset = 0;
    private BufferSource manualPool;
    private FSyncer syncer;
    
    private BufferBuilder createBuffer;
    private final NIOAccessMethod method;
// debugging
    private HashMap<String, Integer> strategies;
    private static final Logger LOGGER = LoggerFactory.getLogger(IOManager.class);
    
    NIOStreamImpl(File filePath, long recommendedSize) throws IOException {
        this(filePath, NIOAccessMethod.getDefault(), recommendedSize, recommendedSize);
    }
    
    NIOStreamImpl(File filePath, NIOAccessMethod method, long recommendedSize, long memorySize) throws IOException {
        directory = filePath;

        if ( LOGGER.isDebugEnabled() ) {
            strategies = new HashMap<String, Integer>();
        }
        segmentSize = recommendedSize;

        LOGGER.debug("==CONFIG(nio)==" + filePath.getAbsolutePath() + " using a segment size of " + (segmentSize / (1024*1024)));
        LOGGER.debug("==CONFIG(nio)==" + filePath.getAbsolutePath() + " using a memory size of " + (memorySize / (1024*1024)));
        
        manualPool = new MaskingBufferSource(new SplittingBufferSource(64,(int)memorySize));
        
        segments = new NIOSegmentList(directory);
        if (segments.isEmpty()) {
            streamId = UUID.randomUUID();
        } else {
            try {
                NIOSegment seg = new NIOSegment(this, segments.getBeginningFile());
                seg.openForHeader();
                streamId = seg.getStreamId();
            } catch ( HeaderException header ) {
                streamId = UUID.randomUUID();
            } catch ( IOException ioe ) {
                streamId = UUID.randomUUID();
            }
        }
        
        this.method = method;
    }
    
    NIORandomAccess createRandomAccess() {
        if ( randomAccess == null ) {
            synchronized ( this) {
                if ( randomAccess == null ) {
                    randomAccess = new NIORandomAccess(this, segments);
                }
            }
        }
        return randomAccess;
    }
    
    private void hintRandomAccess(long marker, int segmentId) {
        if ( randomAccess != null ) {
            randomAccess.hint(marker, segmentId);
        }
    }
    
    NIOAccessMethod getAccessMethod() {
        return method;
    }
    
    public void setBufferBuilder(BufferBuilder builder) {
        createBuffer = builder;
    }
    
    FileBuffer createFileBuffer(FileChannel channel, int bufferSize) throws IOException {         
        FileBuffer created = ( createBuffer != null ) 
                ? createBuffer.createBuffer(channel, this.manualPool, bufferSize)
                : new FileBuffer(channel, this.manualPool, bufferSize);
        
        return created;
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
        NIOSegment seg = new NIOSegment(this, segments.getEndFile());

        try {
                seg.openForHeader();
            } catch ( HeaderException header ) {
                return false;
            }
            if (!seg.getStreamId().equals(streamId)) {
                throw new IOException(BAD_STREAM_ID);
            }
            return seg.wasProperlyClosed();
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
            WritingSegment seg = new WritingSegment(this, f);
            try {
                seg.open();
                if (!seg.getStreamId().equals(streamId)) {
        //  fail,  the stream id does not equal segment's stream id
                    throw new IOException(BAD_STREAM_ID);
                }
                if ( seg.last() ) {
                    updateCurrentMarker(seg.getMaximumMarker());
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
            WritingSegment seg = new WritingSegment(this, f);
            try {
                seg.openForHeader();
                if (!seg.getStreamId().equals(streamId)) {
                    throw new IOException(BAD_STREAM_ID);
                }
                if (seg.getSegmentId() == segment) {
                    segments.removeFilesFromHead();
                    if ( !segments.currentIsHead() ) {
                        throw new IOException("unable to make log stream consistent");
                    }
                    seg.open();
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
        WritingSegment segment = new WritingSegment(this, f);

        try {
            segment.open();

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
        } catch ( HeaderException header ) {
            throw new IOException(header);
        } finally {
            segment.close();
        }
    }  
    
    long findLogTail() throws IOException {
        if ( readHead != null ) {
            throw new AssertionError("read head still active");
        }
        segments.setReadPosition(0);
        File f = segments.nextReadFile(Direction.FORWARD);
        long size = 0;
        while (f != null) {
            NIOSegment seg = new NIOSegment(this, f);

            try {
                    seg.openForHeader();
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
            size += seg.size();
                
            f = segments.nextReadFile(Direction.FORWARD);
        }
        return size;
    }

    long trimLogTail(long timeout) throws IOException {
        if ( findLogTail() != 0 ) { // position the read had over the last dead segment
            File last = segments.getCurrentReadFile();
            assert(last!=null);
            if ( doubleCheck(last) ) {  //  make sure this is the right file, assert?!
                long size = segments.removeFilesFromTail();
                return size;
            }
        }
        return 0;
    }

    @Override
    public long append(Chunk c, long marker) throws IOException {
      if (writeHead == null || writeHead.isClosed()) {
            File f = segments.appendFile();
            
            try {
                writeHead = new WritingSegment(this, f).open();
            } catch ( HeaderException header ) {
                throw new IOException(header);
            }
            
            writeHead.insertFileHeader(lowestMarker, currentMarker+1);
            hintRandomAccess(currentMarker+1, writeHead.getSegmentId());
        }

        long w = writeHead.append(c, marker);
        updateCurrentMarker(marker);
        if (writeHead.size() > segmentSize || c instanceof SnapshotRequest ) {
            closeSegment(writeHead);
        }
        return w;

    }

    class FSyncer extends Thread {

        private Exchanger<WritingSegment> pivot = new Exchanger<WritingSegment>();

        public FSyncer() {
            setName("fsync helper");
            setDaemon(true);
        }

        WritingSegment pivot(WritingSegment target) {
            try {
                return pivot.exchange(target);
            } catch (InterruptedException ie) {
            } 
            return null;
        }

        public void run() {
            try {
                WritingSegment seg = null;
                seg = pivot.exchange(seg);
                while (!Thread.interrupted()) {
                    if ( seg == null ) {
                        
                    } else if (seg.isClosed()) {
                        seg.close();
                    } else {
                        seg.fsync(false);
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
                WritingSegment check = syncer.pivot(null);
                assert(check == writeHead);
                return pos;
            } else {
                pos = writeHead.fsync(false);
                this.fsyncdMarker = writeHead.getMaximumMarker();
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
            closeSegment(writeHead);
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
        
        if ( randomAccess != null ) {
          randomAccess.close();
        }
        randomAccess = null;
            
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

                ReadOnlySegment nextHead = new ReadOnlySegment(this, method, f, dir);
                nextHead.load();
                hintRandomAccess(nextHead.getBaseMarker(), nextHead.getSegmentId());
                                
                if ( readHead != null ) {
                    int expected = readHead.getSegmentId() + ((dir == Direction.REVERSE) ? -1 : +1);
                    if (nextHead.getSegmentId() != expected) {
                        throw new IOException("broken stream during readback expected:" + expected + 
                                " segment:" + segments.getSegmentPosition() + 
                                " actual:" + nextHead.getSegmentId() + " file:"  + nextHead.getFile() + " list:" + 
                                segments.toString());
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
            
            if ( dir == Direction.RANDOM ) {
                Chunk scanned = readHead.scan(offset);
                if ( scanned != null ) {
                    return scanned;
                }
            }

        }

        return readHead.next(dir);
    }
    
    synchronized void waitForMarker(long lsn) throws InterruptedException {
        markerWaiters += 1;
        try {
            while (lsn > this.currentMarker ) {
                this.wait();
            }
 //  just make sure the current data is all the way down to disk before releasing
            if ( lsn > this.fsyncdMarker && this.writeHead != null ) {
                this.writeHead.fsync(true);
                this.fsyncdMarker = this.currentMarker;
            }
        } catch ( IOException ioe ) {
            throw new RuntimeException(ioe);
        } finally {
            markerWaiters -= 1;        
        }
    }
//  mostly called by the IO thread and should not be too expensive so ok to synchronize  
    private synchronized void updateCurrentMarker(long lsn) throws IOException {
        if ( lsn < currentMarker ) {
            throw new IllegalArgumentException("markers must always be increasing");
        }
        currentMarker = lsn;
        if ( markerWaiters > 0 ) {
            this.writeHead.fsync(true);
            this.fsyncdMarker = this.currentMarker;
            if ( this.currentMarker != writeHead.getMaximumMarker() ) {
                throw new AssertionError("IO race");
            }
            this.notifyAll();
        }
    }

    void checkStreamId(NIOSegment segment) throws IOException {
        if (!streamId.equals(segment.getStreamId())) {
            throw new IOException(BAD_STREAM_ID);
        }
    }

    @Override
    public void seek(long loc) throws IOException {
        int segmentId;
        if ( loc > 0 ) {
            segmentId = createRandomAccess().seek(loc).getSegmentId();
            offset = loc;
        } else {
            offset = -1;
            segmentId = (int)loc;  //  ( <= 0 )
        }

        if ( readHead != null ) {
            if ( segmentId != segments.getSegmentPosition() ) {
                readHead.close();
                readHead = null;
            }
        }

        segments.setReadPosition(segmentId);
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

    List<File> fileList() {
        return new ArrayList<File>(segments);
    }

    private void closeSegment(WritingSegment nio) throws IOException {
        nio.prepareForClose();
        if ( syncer != null ) {
          syncer.pivot(nio);
        } else {
          nio.close();
          lowestMarkerOnDisk = nio.getMinimumMarker();
        }
    }
}
