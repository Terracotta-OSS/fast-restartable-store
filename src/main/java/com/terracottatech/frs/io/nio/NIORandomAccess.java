/*
 * All content copyright (c) 2013 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.RandomAccess;
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
class NIORandomAccess implements RandomAccess {
    private final NIOStreamImpl stream;
    private final NIOSegmentList segments;
    private final NavigableMap<Long,Integer> fileIndex;
    private ReadOnlySegment[] fileCache;
    private int segmentOffset = 0;


    NIORandomAccess(NIOStreamImpl stream, NIOSegmentList segments) {
        this.stream = stream;
        this.segments = segments;
        this.fileIndex = new ConcurrentSkipListMap<Long, Integer>();
        this.fileCache = new ReadOnlySegment[1];
        this.segmentOffset = segments.getBeginningSegmentId();
    }

    @Override
    public Chunk scan(long marker) throws IOException {
        Map.Entry<Long,Integer> cacheId = fileIndex.floorEntry(marker);
        int segId = ( cacheId != null ) ? cacheId.getValue() : segments.getBeginningSegmentId();
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
                seg.load();
            }
            c = seg.scan(marker);
        }
        return c;
    }
    
    private ReadOnlySegment findSegment(int segNo) {
        ReadOnlySegment[] list = null;
        int pos = 0;
        synchronized (this) {
            list = this.fileCache;
            pos = segNo - this.segmentOffset;
        }
        if ( pos >= list.length ) {
            return null;
        }
        return list[pos];
    }
    
    private synchronized void removeSegments(int limit) throws IOException {
        if ( limit-segmentOffset == 0 ) {
            return;
        }
        int x =0;
        for (;x<limit-segmentOffset;x++) {
            if ( fileCache[x] != null ) {
                fileCache[x].close();
            }
        }
        fileCache = Arrays.copyOfRange(fileCache, x,fileCache.length);
        segmentOffset = limit;
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
                seg = new ReadOnlySegment(stream, f, Direction.RANDOM);
                addSegment(seg);
            } catch ( HeaderException header ) {
                throw new IOException(header);
            }
        }
        return seg;
    }    
// needs synchronization     
    private void addSegment(ReadOnlySegment ro) {
        if ( !Thread.holdsLock(this) ) {
            throw new AssertionError("needs synchronization");
        }
        if ( fileCache.length <= ro.getSegmentId() - segmentOffset ) {
            fileCache = Arrays.copyOf(fileCache, Math.abs(ro.getSegmentId() - fileCache[0].getSegmentId()) + fileCache.length);
        }
        fileCache[ro.getSegmentId() - segmentOffset] = ro;
    }
}
