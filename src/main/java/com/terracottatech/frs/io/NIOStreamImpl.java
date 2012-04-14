/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.*;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.*;

/**
 * NIO implementation of Log Stream.
 *
 * @author mscott
 */
class NIOStreamImpl implements Stream {

    private final File directory;

    private final long segmentSize;
    private List<File> segments;
    private int position = 0;

    private int lastGoodSegment = Integer.MAX_VALUE;
    private long lastGoodPosition = Long.MAX_VALUE;
    private UUID   streamid;
    
    private static final String format = "seg%09d.frs";
    private static final String segNumFormat = "000000000";
    private static final String BAD_STREAMID = "mis-aligned streams";
    private NIOSegmentImpl currentSegment;
    private final ChunkSource   pool = new ChunkSource();

    public NIOStreamImpl(File filepath, long recommendedSize) throws IOException {
        directory = filepath;
        
        segmentSize = recommendedSize;

        enumerateSegments();
    } 
    
    int convertSegmentNumber(File f) {
        try {
            return new DecimalFormat(segNumFormat).parse(f.getName().substring(3,f.getName().length()-4)).intValue();
        } catch ( ParseException pe ) {
            throw new RuntimeException("bad filename",pe);
        }
    }
    
    @Override
    public UUID getStreamId() {
        return streamid;
    }

    
    private void enumerateSegments() throws IOException {                    
        segments = Arrays.asList(
            directory.listFiles(new FilenameFilter() {

                @Override
                public boolean accept(File file, String string) {
                    if ( file.length() < 26 ) {
                        return false;
                    }
                    return string.startsWith("seg");
                }
            })
        );
        
        segments = new ArrayList(segments);
        Collections.sort(segments);        
        
        while ( segments.size() > 0 ) {
            int segNum = convertSegmentNumber(segments.get(segments.size()-1));
            if ( segNum > lastGoodSegment ) {
                segments.remove(segments.size()-1);
            } else {
                break;
            }
        }
        
        if ( streamid == null && segments.isEmpty() ) streamid = UUID.randomUUID();
        
        position = segments.size()-1;
        
    }
    
    public void shutdown() {
        try {
            close();
        } catch (IOException io) {
            throw new AssertionError(io);
        }
    }
    
//  probably doesn't need to be synchronized.  only IO thread should be calling

    @Override
    public synchronized Segment append() throws IOException {
        StringBuilder fn = new StringBuilder();
        Formatter pfn = new Formatter(fn);
        int number = 0;
        if ( currentSegment == null && !segments.isEmpty() ) {
            currentSegment = new NIOSegmentImpl(this, Direction.getDefault(), segments.get(segments.size()-1), segmentSize).openForReading(pool);
            streamid = currentSegment.getStreamId();
        }
        if ( currentSegment != null ) {
            assert(currentSegment.getSegmentId() == convertSegmentNumber(segments.get(position)));
            if ( !currentSegment.isClosed() ) currentSegment.close();
            number = currentSegment.getSegmentId() + 1;
        }
        pfn.format(format, number);

        if ( segments == null ) segments = new ArrayList<File>();
        File nf = new File(directory, fn.toString());
        position = segments.size();
        segments.add(nf);

        currentSegment = new NIOSegmentImpl(this, Direction.getDefault(), nf, segmentSize).openForWriting();
        
        return currentSegment;
    }
    //  fsync current segment.  old segments are fsyncd on close

    @Override
    public long sync() throws IOException {
        if (currentSegment != null && !currentSegment.isClosed()) {
            return currentSegment.fsync();
        }            
        return -1;
    }
    //  segment implementation forces before close.  neccessary?

    @Override
    public void close() throws IOException {
        if (currentSegment != null && !currentSegment.isClosed()) {
            currentSegment.close();
        }
    }

    @Override
    public Segment read(final Direction dir) throws IOException {
        long setsize = Long.MAX_VALUE;
        UUID lastStreamId = ( currentSegment != null ) ? currentSegment.getStreamId() : null;
 
        if (position == segments.size()-1) {
            setsize = lastGoodPosition;
            assert(lastGoodSegment == Integer.MAX_VALUE || convertSegmentNumber(segments.get(position)) == lastGoodSegment);
        }
        
        if ( dir == Direction.FORWARD ) {
            if ( position > segments.size() - 1 ) return null;
            currentSegment = new NIOSegmentImpl(this, dir, segments.get(position++), setsize).openForReading(pool);
        } else {
            if ( position < 0 ) return null;
            currentSegment = new NIOSegmentImpl(this, dir, segments.get(position--), setsize).openForReading(pool);
        }
        
        if (lastStreamId != null && !lastStreamId.equals(currentSegment.getStreamId())) {
            throw new IOException(BAD_STREAMID);
        }
        
        if ( streamid == null ) streamid = currentSegment.getStreamId();
        
        return currentSegment;
    }

    @Override
    public void seek(long loc) throws IOException {
        if ( loc < 0 ) {
            position = segments.size()-1;
        } else if ( loc == 0 ) {
            position = 0;
        }
        currentSegment = null;
    }
    
    
}
