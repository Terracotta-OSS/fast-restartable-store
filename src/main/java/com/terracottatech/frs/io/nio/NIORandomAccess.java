/*
 * All content copyright (c) 2013 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

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

/**
 *
 * @author mscott
 */
class NIORandomAccess implements RandomAccess, Closeable {
    private final NIOStreamImpl stream;
    private final NIOSegmentList segments;
    private final NavigableMap<Long,Integer> fileIndex;
    private volatile FileCache cache;

    NIORandomAccess(NIOStreamImpl stream, NIOSegmentList segments) {
        this.stream = stream;
        this.segments = segments;
        this.fileIndex = new ConcurrentSkipListMap<Long, Integer>();
        this.cache = new FileCache(segments.getBeginningSegmentId(),new ReadOnlySegment[1]);
    }

    @Override
    public Chunk scan(long marker) throws IOException {
        Map.Entry<Long,Integer> cacheId = fileIndex.floorEntry(marker);
        int segId = ( cacheId != null ) ? cacheId.getValue() : cache.getOffset();
        Chunk c = null;
        while ( c == null ) {
            int getId = segId++;
            ReadOnlySegment seg = findSegment(getId);
            if ( seg == null ) {
                seg = createSegment(getId);
            }
            if ( seg == null ) {
                return null;
            } else {
                if ( marker < seg.load().getBaseMarker() ) {
                    throw new AssertionError("overshoot: " + marker + " < " + seg.getBaseMarker());
                }
            }
            c = seg.scan(marker);
        }
        return c;
    }

    @Override
    public void close() throws IOException {
      cache.close();
    }
 
    private ReadOnlySegment findSegment(int segNo) {
        return cache.findSegment(segNo);
    }
    
    private void removeSegments(int limit) throws IOException {
        if ( limit-cache.getOffset() == 0 ) {
            return;
        }
        
        cache = cache.removeSegments(limit);
    }    
    
    ReadOnlySegment seek(long marker) throws IOException {
        Map.Entry<Long,Integer> cacheId = fileIndex.floorEntry(marker);
        int segId = ( cacheId != null ) ? cacheId.getValue() : segments.getBeginningSegmentId();
        ReadOnlySegment seg = null;
        while ( seg == null ) {
            int getId = segId++;
            seg = findSegment(getId);
            if ( seg == null ) {
                seg = createSegment(getId);
            }
            if ( seg != null ) {
                seg.load();
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
        
        removeSegments(fid);
    }
    
    void hint(long marker, int segment) {
        fileIndex.put(marker, segment);
    }
    
    private synchronized ReadOnlySegment createSegment(int segId) throws IOException {
        ReadOnlySegment seg = findSegment(segId);
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
    
    private static class FileCache implements Closeable {
        private final int offset;
        private final ReadOnlySegment[] segments;

        public FileCache(int offset, ReadOnlySegment[] segments) {
            this.offset = offset;
            this.segments = segments;
        }
        
        public ReadOnlySegment findSegment(int segno) {
            int pos = segno - offset;
            if ( pos >= segments.length ) {
                return null;
            }
            return segments[pos];
        }

        public int getOffset() {
            return offset;
        }

 // under lock       
        public FileCache removeSegments(int limit) throws IOException {
            int x =0;
            if ( limit > segments.length - getOffset() ) {
                limit = segments.length - getOffset();
            }
            for (;x<limit-getOffset();x++) {
                if ( segments[x] != null ) {
                    segments[x].close();
                    segments[x] = null;
                }
            }
            return new FileCache(limit,Arrays.copyOfRange(segments, x,segments.length));
        }
 // under lock       
        public FileCache addSegment(ReadOnlySegment ro) {
            if ( segments.length <= ro.getSegmentId() - offset ) {
                ReadOnlySegment[] na = Arrays.copyOf(segments, Math.abs(ro.getSegmentId() - offset) + segments.length + 1);
                na[ro.getSegmentId() - offset] = ro;
                return new FileCache(offset,na);  // buffer by adding the current length to the required length + 1
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
