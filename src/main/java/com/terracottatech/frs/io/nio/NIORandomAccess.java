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
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 *
 * @author mscott
 */
class NIORandomAccess implements RandomAccess {
    private final NIOStreamImpl stream;
    private final NIOSegmentList segments;
    private final NavigableMap<Long,Integer> fileIndex;
    private final Map<Integer,ReadOnlySegment> fileCache;


    NIORandomAccess(NIOStreamImpl stream, NIOSegmentList segments) {
        this.stream = stream;
        this.segments = segments;
        this.fileIndex = new ConcurrentSkipListMap<Long, Integer>();
        this.fileCache = new ConcurrentHashMap<Integer,ReadOnlySegment>();
    }

    @Override
    public Chunk scan(long marker) throws IOException {
        Map.Entry<Long,Integer> cacheId = fileIndex.floorEntry(marker);
        int segId = ( cacheId != null ) ? cacheId.getValue() : segments.getBeginningSegmentId();
        Chunk c = null;
        while ( c == null ) {
            int getId = segId++;
            ReadOnlySegment seg = fileCache.get(getId);
            if ( seg == null ) {
                seg = createSegment(getId);
            }
            if ( seg == null ) {
                return null;
            }
            c = seg.scan(marker);
        }
        return c;
    }
    
    ReadOnlySegment seek(long marker) throws IOException {
        Map.Entry<Long,Integer> cacheId = fileIndex.floorEntry(marker);
        int segId = ( cacheId != null ) ? cacheId.getValue() : segments.getBeginningSegmentId();
        ReadOnlySegment seg = null;
        while ( seg == null ) {
            int getId = segId++;
            seg = fileCache.get(getId);
            if ( seg == null ) {
                seg = createSegment(getId);
            }
            if ( seg != null && seg.getMaximumMarker() >= marker ) {
                break;
            } else {
                seg = null;
            }
        }
        return seg;
    }    
    
    private void cleanCache() throws IOException {
        int fid = segments.getBeginningSegmentId();
        
        Map.Entry<Long,Integer> first = this.fileIndex.firstEntry();
        
        while (first != null && fid > first.getValue()) {
            ReadOnlySegment ro = fileCache.remove(fileIndex.remove(first.getKey()));
            if ( ro != null ) {
                ro.close();
            }
        }
    }
    
    void hint(long marker, int segment) {
        fileIndex.put(marker, segment);
    }
    
     synchronized ReadOnlySegment createSegment(int segId) throws IOException {
        ReadOnlySegment seg = fileCache.get(segId);
        if ( seg == null ) {
            // first clear any old caches not needed anymore
            cleanCache();
            try {
                File f = segments.getFile(segId);
                if ( f == null ) {
                    return null;
                }
                seg = new ReadOnlySegment(stream, f, Direction.RANDOM);
                fileCache.put(segId, seg);
            } catch ( HeaderException header ) {
                throw new IOException(header);
            }
        }
        return seg;
    }     
}
