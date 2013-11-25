/*
 * All content copyright (c) 2013 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.RandomAccess;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mscott
 */
class NIORandomAccess implements RandomAccess, Closeable {
    private final NIOStreamImpl stream;
    private final NIOSegmentList segments;
    private final NavigableMap<Long,Integer> fileIndex;
    private volatile FileCache cache;
    private int maxFiles = Integer.MAX_VALUE;
    private final BufferSource src;
    private static final Logger LOGGER = LoggerFactory.getLogger(RandomAccess.class);


    NIORandomAccess(NIOStreamImpl stream, NIOSegmentList segments, BufferSource src) {
        this.stream = stream;
        this.segments = segments;
        this.fileIndex = new ConcurrentSkipListMap<Long, Integer>();
        this.cache = new FileCache(segments.getBeginningSegmentId(),0,new ReadOnlySegment[1]);
        this.src = src;
    }
    
    public void setMaxFiles(int size) {
      maxFiles = size;
    }

    @Override
    public Chunk scan(long marker) throws IOException {
        Map.Entry<Long,Integer> cacheId = fileIndex.floorEntry(marker);
        int segId = ( cacheId != null ) ? cacheId.getValue() : cache.getOffset();
        Chunk c = null;
        while ( c == null ) {
            ReadOnlySegment seg = findSegment(segId);
            if ( seg == null ) {
                seg = createSegment(segId);
            }
            if ( seg == null ) {
                return null;
            } else {
                if ( marker < seg.load(src).getBaseMarker() ) {
                    throw new AssertionError("overshoot: " + marker + " < " + seg + " " + cacheId + " " + segId + " " + cache.getOffset() + " " + fileIndex);
                }
            }
            c = seg.scan(marker);
            if ( c == null && LOGGER.isDebugEnabled() ) {
              LOGGER.debug(marker + " " + seg);
            }
            if ( c == null ) {
              if ( seg.isComplete() ) {
                segId += 1;
              } else if ( LOGGER.isDebugEnabled() ) {
                if ( segments.getCount() + segments.getBeginningSegmentId() != seg.getSegmentId() ) {
                  throw new AssertionError();
                }
                LOGGER.debug("not advanced " + segId);
              }
            }
        }
        return c;
    }

    @Override
    public void close() throws IOException {
      cache.close();
    }
 
    private ReadOnlySegment findSegment(int segNo) throws IOException {
        ReadOnlySegment ro = cache.findSegment(segNo);
        if ( ro == null ) {
            ro = createSegment(segNo);
        }
        if ( ro.getSegmentId() != segNo ) {
          throw new AssertionError();
        }
        return ro;
    }  
    
    ReadOnlySegment seek(long marker) throws IOException {
        Map.Entry<Long,Integer> cacheId = fileIndex.floorEntry(marker);
        int segId = ( cacheId != null ) ? cacheId.getValue() : segments.getBeginningSegmentId();
        ReadOnlySegment seg = null;
        while ( seg == null ) {
            int getId = segId++;
            seg = findSegment(getId);
            if ( seg != null ) {
                seg.load(src);
                if ( seg.getMaximumMarker() >= marker ) {
                    break;
                } else {
                    seg = null;
                }
            }
        }
        return seg;
    }    
    
    private void cleanCache() throws IOException {
        int fid = segments.getBeginningSegmentId();
        
        Map.Entry<Long,Integer> first = this.fileIndex.firstEntry();
        
        while (first != null && fid > first.getValue()) {
            fileIndex.remove(first.getKey());
            first = this.fileIndex.firstEntry();
        }
        
        cache = cache.removeSegments(fid);
    }
    
    void hint(long marker, int segment) {
        fileIndex.put(marker, segment);
    }

    private synchronized ReadOnlySegment createSegment(int segId) throws IOException {
        ReadOnlySegment seg = cache.findSegment(segId);
        if ( seg == null ) {
            // first clear any old caches not needed anymore
            cleanCache();
            try {
                File f = segments.getFile(segId);
                if ( f == null ) {
                    return null;
                }
                seg = new ReadOnlySegment(stream, stream.getAccessMethod(), f, Direction.RANDOM);
                cache = cache.addSegment(seg);
            } catch ( HeaderException header ) {
                throw new IOException(header);
            }
        }
        return seg;
    }    
    
    private class FileCache implements Closeable {
        private final int offset;
        private int livecount = 0;
        private final ReadOnlySegment[] segments;

        public FileCache(int offset, int live, ReadOnlySegment[] segments) {
            this.offset = offset;
            this.livecount = live;
            this.segments = segments;
            for (int x=0;x<segments.length;x++) {
              if ( segments[x] != null ) {
                if ( segments[x].getSegmentId() != offset + x ) {
                  throw new AssertionError();
                }
              }
            }
        }
        
        public ReadOnlySegment findSegment(int segno) {
            int pos = segno - offset;
            if ( pos >= segments.length ) {
                return null;
            }
            ReadOnlySegment ro = segments[pos];
            if ( ro != null && ro.getSegmentId() != segno ) {
              throw new AssertionError(segno + " " + segments[pos].getSegmentId());
            }
            return ro;
        }

        public int getOffset() {
            return offset;
        }

 // under lock       
        public FileCache removeSegments(int limit) throws IOException {
            if ( limit <= offset ) {
              return this;
            }
            if ( limit > offset + segments.length ) {
                limit = offset + segments.length;
            }
            int x = 0;
            for (;x + offset < limit;x++) {
                if ( segments[x] != null ) {
                    segments[x].close();
                    segments[x] = null;
                    livecount--;
                }
            }
            return new FileCache(limit,livecount,Arrays.copyOfRange(segments, x,segments.length+1));
        }
 // under lock       
        public FileCache addSegment(ReadOnlySegment ro) throws IOException {
            if ( ro.getSegmentId() < offset ) {
              throw new AssertionError(ro.getSegmentId() + " " + offset);
            }
            if (livecount++ > maxFiles ) {
              for ( int k=0;k<segments.length;k++ ) {
                if ( segments[k] != null ) {
                  segments[k].close();
                  segments[k] = null;
                  livecount--;
                  break;
                }
              }
            }
            if ( segments.length <= ro.getSegmentId() - offset ) {
                ReadOnlySegment[] na = Arrays.copyOf(segments, ro.getSegmentId() - offset + segments.length + 1);
                na[ro.getSegmentId() - offset] = ro;
                return new FileCache(offset,livecount,na);  // buffer by adding the current length to the required length + 1
            }
            segments[ro.getSegmentId() - offset] = ro;
            return this;
        }
        
        @Override
        public void close() throws IOException {
          for ( ReadOnlySegment ro : segments ) {
            if ( ro != null ) {
              ro.close();
            }
          }
        }
    
    }
}
