/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.*;
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

    private UUID   streamid;
    
    private static final String format = "seg%09d.frs";
    private static final String segNumFormat = "000000000";
    private static final String BAD_STREAMID = "mis-aligned streams";
    private NIOSegmentImpl      currentSegment;
    private final BufferSource  pool = new AllocatingBufferSource();

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
    
    void limit(UUID streamid, int segment,long position) throws IOException {
        for (int x=segments.size()-1;x>=0;x--) {
            NIOSegmentImpl seg = new NIOSegmentImpl(this, Direction.getDefault(), segments.get(x), segmentSize);
            try {
                seg.openForReading(pool);
                if ( seg.getStreamId().equals(streamid) ) throw new IOException(BAD_STREAMID);
                if ( seg.getSegmentId() > segment ) {
                    segments.get(x).delete();
                    segments.remove(x);
                }
                if ( seg.getSegmentId() == segment ) {
                    seg.limit(position);
                    return;
                }
            } finally {
                seg.close();
            }
        }
    }
    
    private void enumerateSegments() throws IOException {                    
        segments = Arrays.asList(
            directory.listFiles(new FilenameFilter() {

                @Override
                public boolean accept(File file, String string) {
                    if ( file.length() < NIOSegmentImpl.FILE_HEADER_SIZE ) {
                        return false;
                    }
                    return string.startsWith("seg");
                }
            })
        );
        
        segments = new ArrayList(segments);
        Collections.sort(segments);        
        
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
    
    @Override
    public long append(Chunk c) throws IOException {
        StringBuilder fn = new StringBuilder();
        Formatter pfn = new Formatter(fn);
        int number = 0;
        
        if ( currentSegment == null || currentSegment.isClosed() ) {
            if ( currentSegment == null && !segments.isEmpty() ) {
                currentSegment = new NIOSegmentImpl(this, Direction.getDefault(), segments.get(segments.size()-1), segmentSize).openForReading(pool);
                streamid = currentSegment.getStreamId();
            }

            if ( currentSegment != null ) {
                assert(currentSegment.getFile().equals(segments.get(segments.size()-1)));
                assert(currentSegment.getSegmentId() == convertSegmentNumber(segments.get(position)));
                number = currentSegment.getSegmentId() + 1;
            }

            pfn.format(format, number);

            if ( segments == null ) segments = new ArrayList<File>();
            File nf = new File(directory, fn.toString());
            position = segments.size();
            segments.add(nf);

            currentSegment = new NIOSegmentImpl(this, Direction.getDefault(), nf, segmentSize).openForWriting(pool);
        }
        
        long w = currentSegment.append(c);
        if ( currentSegment.length() > segmentSize) currentSegment.close();
        return w;
        
        
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
        currentSegment = null;
    }

    @Override
    public Chunk read(final Direction dir) throws IOException {
        if ( currentSegment != null ) {
            UUID lastStreamId = currentSegment.getStreamId();
            
            if (lastStreamId != null && !lastStreamId.equals(currentSegment.getStreamId())) {
                throw new IOException(BAD_STREAMID);
            }
        }
        
        if ( currentSegment == null || !currentSegment.hasMore(dir) ) {
            if ( currentSegment != null ) currentSegment.close();
            pool.reclaim();

            if ( dir == Direction.FORWARD ) {
                if ( position > segments.size() - 1 ) return null;
                currentSegment = new NIOSegmentImpl(this, dir, segments.get(position++), segmentSize).openForReading(pool);
            } else {
                if ( position < 0 ) return null;
                currentSegment = new NIOSegmentImpl(this, dir, segments.get(position--), segmentSize).openForReading(pool);
            }
        }        
        
        if ( streamid == null ) streamid = currentSegment.getStreamId();
        
        if ( !currentSegment.hasMore(dir) ) return null;
        return currentSegment.next(dir);            
    }

    @Override
    public void seek(long loc) throws IOException {
        if ( loc < 0 ) {
            position = segments.size()-1;
        } else if ( loc == 0 ) {
            position = 0;
        }
        if ( currentSegment != null ) currentSegment.close();
        currentSegment = null;
    }
    
    public int getSegmentId() {
        return currentSegment.getSegmentId();
    }

    @Override
    public Iterator<Chunk> iterator() {
        return new Iterator<Chunk>() {
            
            Chunk next;

            @Override
            public boolean hasNext() {
                try {
                    if ( next != null ) return true;
                    next = read(Direction.getDefault());
                    return ( next != null );
                } catch ( IOException ioe ) {
                    throw new RuntimeException(ioe);
                }
            }

            @Override
            public Chunk next() {
                if ( !hasNext() ) throw new IndexOutOfBoundsException();
                return next;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        };
    }
    
    
}
