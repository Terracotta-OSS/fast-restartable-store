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
import com.terracottatech.frs.SnapshotRequest;
import com.terracottatech.frs.io.BufferBuilder;
import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.FileBuffer;
import com.terracottatech.frs.io.HeapBufferSource;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.Stream;
import com.terracottatech.frs.util.Log2LatencyBins;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * NIO implementation of Log Stream.
 *
 * @author mscott
 */
class NIOStreamImpl implements Stream {
    /*  concurrency needed for cloning */
    private final static Integer REPORT_FSYNC_LATENCIES = Integer.getInteger(
      "com.terracottatech.frs.ReportFsyncLatenciesInSecs",
      (int) TimeUnit.SECONDS.convert(5, TimeUnit.MINUTES));
    private final NIOSegmentList          segments;
    private final Log2LatencyBins fsyncLatencyBin;
    private final Thread reporterThread;
    private volatile NIORandomAccess      randomAccess;

    static final String BAD_STREAM_ID = "mis-aligned streams";
    private final File directory;
    private final long segmentSize;
    
    private boolean syncDisabled = false;

    private UUID streamId;
    private volatile long lowestMarker = Constants.GENESIS_LSN;
    private volatile long lowestMarkerOnDisk = 0;
    private volatile long fsyncdMarker = 0;
    private volatile long currentMarker = Constants.GENESIS_LSN;  //  init lsn is 100
    private int  markerWaiters = 0;
    private WritingSegment writeHead;
    private ReadOnlySegment readHead;
    private long offset = 0;
    private final BufferSource filePool;
    private BufferSource replayPool;
    private FSyncer syncer;
    private volatile boolean closed = false;
    private BufferBuilder createBuffer;
    private final NIOAccessMethod method;
// debugging
    private HashMap<String, Integer> strategies;
    private static final Logger LOGGER = LoggerFactory.getLogger(NIOStreamImpl.class);
    private AtomicBoolean reporterShutdown=new AtomicBoolean(false);

    NIOStreamImpl(File filePath, long recommendedSize) throws IOException {
        this(filePath, NIOAccessMethod.getDefault(), recommendedSize, new HeapBufferSource(512 * 1024 * 1024), null);
    }
    
    NIOStreamImpl(File filePath, NIOAccessMethod method, long recommendedSize, BufferSource writeBuffers, BufferSource recoveryBuffers) throws IOException {
        directory = filePath;
        this.fsyncLatencyBin = new Log2LatencyBins("FRS Sync: " + directory);

        filePool = writeBuffers;
        replayPool = recoveryBuffers;
        if ( replayPool == null ) {
          replayPool = filePool;
        }
        
        if ( LOGGER.isDebugEnabled() ) {
            strategies = new HashMap<String, Integer>();
        }
        this.segmentSize = recommendedSize;

        LOGGER.debug("==CONFIG(nio)==" + filePath.getAbsolutePath() + " using a segment size of " + (segmentSize / (1024*1024)));
                
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

        if (REPORT_FSYNC_LATENCIES != null && REPORT_FSYNC_LATENCIES.intValue() > 0) {
            LOGGER.info("Reporting fsync latencies every " + REPORT_FSYNC_LATENCIES + " seconds");
            this.reporterThread = fsyncLatencyBin.reporterThread(makeReporterShutdownSupplier(this.reporterShutdown),
                                                                 REPORT_FSYNC_LATENCIES.intValue(),
                                                                 TimeUnit.SECONDS,
                                                                 0,
                                                                 makeLatencyConsumer(fsyncLatencyBin));
            this.reporterThread.start();

        } else {
            this.reporterThread = null;
        }
    }

    private static BooleanSupplier makeReporterShutdownSupplier(AtomicBoolean signal) {
        return () -> (!signal.get());
    }

    private static Consumer<Log2LatencyBins> makeLatencyConsumer(Log2LatencyBins fsyncLatencyBin) {
        return b -> {
            LOGGER.info(b.toString(Log2LatencyBins.ToString.NO_RANGES_AND_ZEROS, b.binCounts(), b.count()));
            fsyncLatencyBin.sloppyReset();
        };
    }

    NIORandomAccess createRandomAccess(BufferSource src) {
        if ( randomAccess == null ) {
            synchronized ( this) {
                if ( randomAccess == null ) {
                    randomAccess = new NIORandomAccess(this, segments, src);
                }
            }
        }
        return randomAccess;
    }
    
    void disableSync(boolean disabled) {
      syncDisabled = disabled;
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
                ? createBuffer.createBuffer(channel, this.filePool, bufferSize)
                : new FileBuffer(channel, this.filePool, bufferSize);
        
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

    public long getSyncdMarker() {
        return fsyncdMarker;
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
        
        if( !segments.isEmpty() ) {
//  assume we are going to recover these files, create memory for recovery
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
                      updateSyncMarker(seg.getMaximumMarker());
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
    
    long scanForEnd() throws IOException {
        int count = segments.size();
        long size = 0;
        for (int x=0;x<count;x++) {
            File f = segments.get(x);
            if ( f == null || !f.exists() ) {
              break;
            }
            
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
                break;
            }

            // ran into the head.  we are done.
            if ( f.equals(segments.getEndFile()) ) {
                break;
            }
            size += seg.size();                
        }
        return size;
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
                if ( randomAccess != null ) {
                  randomAccess.closeToReadHead();
                }
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
                    updateSyncMarker(seg.getMaximumMarker());
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
        if ( syncDisabled ) {
          return -2;
        }
        if (writeHead != null && !writeHead.isClosed()) {
            if (this.currentMarker == this.fsyncdMarker) {
              return writeHead.position();
            } else if (syncer != null) {
                syncer.pivot(writeHead);
            //  get it back
                WritingSegment check = syncer.pivot(null);
                assert(check == writeHead);
                return  writeHead.position();
            } else {
                long pos = writeHead.fsync(false);
                updateSyncMarker(writeHead.getMaximumMarker());
                this.lowestMarkerOnDisk = writeHead.getMinimumMarker();
                return pos;
            }
        } else {
          LOGGER.debug("no sync on a closed stream");
        }
        
        return -1;
    }
    //  segment implementation forces before close.  neccessary?

    @Override
    public void close() throws IOException {
        closed = true;
        reporterShutdown.set(true);

        if (writeHead != null && !writeHead.isClosed()) {
            closeSegment(writeHead);
        }
        writeHead = null;
        if (readHead != null && !readHead.isClosed()) {
            readHead.close();
        }
        readHead = null;

        if (this.reporterThread != null) {
            this.reporterThread.interrupt();
        }

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
            LOGGER.debug("==PERFORMANCE(memory)==" + filePool.toString());
            StringBuilder slist = new StringBuilder();
            for ( Map.Entry<String,Integer> e : strategies.entrySet() ) {
                slist.append(" ");
                slist.append(e.getKey());
                slist.append(":");
                slist.append(e.getValue());
            }
            LOGGER.debug("==PERFORMANCE(strategies)==" + slist.toString());
        }
       
        filePool.reclaim();  
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
                nextHead.load(replayPool);
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
    
    private synchronized void updateSyncMarker(long marker) {
      this.fsyncdMarker = marker;
      if ( this.currentMarker != this.fsyncdMarker ) {
          throw new AssertionError("IO race");
      }
      if (markerWaiters > 0) {
        notifyAll();
      }
    }
    
    boolean waitForWriteOf(long lsn) throws InterruptedException, IOException {
        if (lsn > this.currentMarker) {
            synchronized (this) {
                markerWaiters += 1;
                try {
                    while (lsn > this.currentMarker) {
                        wait();
                    }
                } finally {
                    markerWaiters -= 1;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    private void updateCurrentMarker(long lsn) throws IOException {
        if ( lsn < currentMarker ) {
            throw new IllegalArgumentException("markers must always be increasing");
        }
        currentMarker = lsn;
        if (markerWaiters > 0) {
            synchronized (this) {
                notifyAll();
            }
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
        if ( loc == IOManager.Seek.BEGINNING.getValue() ) {
       //   recovery done.  we could use this memory
          replayPool = null;
        } 
        
        if ( loc > 0 ) {
            NIORandomAccess ra = createRandomAccess(filePool);
            try {
              this.waitForWriteOf(loc);
              ReadOnlySegment ro = ra.seek(loc);
              if ( ro == null ) {
                throw new IOException("bad seek");
              }
              segmentId = ro.getSegmentId();
              offset = loc;
            } catch ( InterruptedException ie ) {
              throw new InterruptedIOException();
            }
        } else {
            offset = -1;
            segmentId = (int)loc;  //  ( <= 0 )
        }

        if ( readHead != null ) {
            if ( offset < 0 || segmentId != segments.getSegmentPosition() ) {
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
      return Collections.unmodifiableList(segments.copyList());
    }

    private void closeSegment(WritingSegment nio) throws IOException {
        nio.prepareForClose();
        if ( syncer != null ) {
          syncer.pivot(nio);
        } else {
          nio.close();
          this.currentMarker = nio.getMaximumMarker();
          updateSyncMarker(this.currentMarker);
          lowestMarkerOnDisk = nio.getMinimumMarker();
        }
    }

    public void recordFsyncLatency(long ns) {
        if (fsyncLatencyBin != null) {
            fsyncLatencyBin.record(ns);
        }
    }
}
