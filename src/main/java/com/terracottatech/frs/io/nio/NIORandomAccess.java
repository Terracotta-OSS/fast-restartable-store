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
    private static final Logger LOGGER = LoggerFactory.getLogger(NIORandomAccess.class);


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
// for tests
    void seedCache(FileCache cache) {
      this.cache = cache;
    }
// for tests
    FileCache createCache(int offset, int length, ReadOnlySegment[] cache) {
      return new FileCache(offset, length, cache);
    }

    @Override
    public Chunk scan(long marker) throws IOException {
        Map.Entry<Long,Integer> cacheId = fileIndex.floorEntry(marker);
        int segId = ( cacheId != null ) ? cacheId.getValue() : cache.getOffset();
        int startId = segId;
        Chunk c = null;
        while ( c == null ) {
            ReadOnlySegment seg = findSegment(segId);
            if ( seg == null ) {
                return null;
            } else {
                if ( marker < seg.load(src).getBaseMarker() ) {
                  LOGGER.info("overshoot: " + marker + " < " + seg + " " + startId + " " + cacheId + " " + segId + " " + cache.getOffset() + " " + fileIndex);
                  return null;        // Let NIOManager re-drive
                }
            }
            boolean segComplete = seg.isComplete();     // null result from "incomplete" segment is re-driven
            c = seg.scan(marker);
            if ( c == null ) {
              if ( LOGGER.isDebugEnabled() ) {
                LOGGER.debug(marker + " " + seg);
              }
              if ( segComplete ) {
                if (marker > seg.getMaximumMarker()) {
                  segId += 1;
                }
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
        if ( ro != null && ro.getSegmentId() != segNo ) {
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
            if ( seg == null ) {
//  segment overflow
                return null;
            } else {
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
    
    synchronized void closeToReadHead() throws IOException {
      int current = segments.getSegmentPosition();
        
        cache = cache.closeSegments(current);
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
    
    class FileCache implements Closeable {
        private final int offset;
        private int livecount = 0;
        private final ReadOnlySegment[] segments;

        FileCache(int offset, int live, ReadOnlySegment[] segments) {
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
       public FileCache closeSegments(int limit) throws IOException {
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
            return this;
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
