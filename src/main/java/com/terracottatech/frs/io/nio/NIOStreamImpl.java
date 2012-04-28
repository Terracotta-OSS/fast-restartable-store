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
    static final FilenameFilter SEGMENT_FILENAME_FILTER = new FilenameFilter() {
      @Override
      public boolean accept(File file, String string) {
        return string.startsWith("seg") && string.endsWith(".frs");
      }
    };

    private static final String SEGMENT_NAME_FORMAT = "seg%09d.frs";
    private static final String SEG_NUM_FORMAT = "000000000";
    private static final String BAD_STREAM_ID = "mis-aligned streams";

    private final File directory;

    private final long segmentSize;
    private List<File> segments;
    private int position = 0;

    private UUID streamId;
    

    private NIOSegmentImpl      currentSegment;
    private final RotatingBufferSource  pool = new RotatingBufferSource();
    
    private long debugIn;
    private long debugOut;

    public NIOStreamImpl(File filePath, long recommendedSize) throws IOException {
        directory = filePath;
        
        segmentSize = recommendedSize;

        enumerateSegments();
    } 
    
    int convertSegmentNumber(File f) {
        try {
            return new DecimalFormat(SEG_NUM_FORMAT).parse(f.getName().substring(3, f.getName().length() - 4)).intValue();
        } catch ( ParseException pe ) {
            throw new RuntimeException("bad filename",pe);
        }
    }
    
    @Override
    public UUID getStreamId() {
        return streamId;
    }
    
    boolean checkForCleanExit() throws IOException {
        if ( segments.size() == 0 ) return true;
        NIOSegmentImpl seg = new NIOSegmentImpl(this, segments.get(segments.size()-1));
        try {
            pool.reclaim();
            seg.openForReading(pool);
            if ( seg.getStreamId().equals(streamId) ) 
                throw new IOException(BAD_STREAM_ID);
            return seg.wasProperlyClosed();
        } finally {
            seg.close();
        }
    }
    
    void limit(UUID streamId, int segment,long position) throws IOException {
        for (int x=segments.size()-1;x>=0;x--) {
            NIOSegmentImpl seg = new NIOSegmentImpl(this, segments.get(x));
            try {
                pool.reclaim();
                seg.openForReading(pool);
                if ( seg.getStreamId().equals(streamId) ) 
                    throw new IOException(BAD_STREAM_ID);
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
        File[] list = directory.listFiles(SEGMENT_FILENAME_FILTER);
        if ( list == null ) list = new File[0];
        segments = Arrays.asList(list);
        
        segments = new ArrayList<File>(segments);
        Collections.sort(segments);        
        
        if ( segments.isEmpty() ) {
            streamId = UUID.randomUUID();
        } else {
//  use the first segment to get the streamId.  if that is no good, the stream is no good.
            currentSegment = new NIOSegmentImpl(this, segments.get(0)).openForReading(pool);
            streamId = currentSegment.getStreamId();
            currentSegment.close();
        }
        
        position = segments.size()-1;
        
//        System.out.println("u:" + directory.getUsableSpace());
//        System.out.println("t:" + directory.getTotalSpace());
//        System.out.println("f:" + directory.getFreeSpace());
    }
    
    @Override
    public long append(Chunk c) throws IOException {
        StringBuilder fn = new StringBuilder();
        Formatter pfn = new Formatter(fn);
        int number = 0;
                
        if ( currentSegment == null || currentSegment.isClosed() ) {
            if ( currentSegment == null && !segments.isEmpty() ) {
                currentSegment = new NIOSegmentImpl(this, segments.get(segments.size()-1)).openForReading(pool);
                currentSegment.close();
            }
            
            if ( currentSegment != null ) {
                assert(currentSegment.getFile().equals(segments.get(segments.size()-1)));
                assert(currentSegment.getSegmentId() == convertSegmentNumber(segments.get(position)));
                number = currentSegment.getSegmentId() + 1;
            }

            pfn.format(SEGMENT_NAME_FORMAT, number);

            File nf = new File(directory, fn.toString());
            position = segments.size();
            segments.add(nf);
            
            pool.reclaim();
            currentSegment = new NIOSegmentImpl(this, nf).openForWriting(pool);
        }
        
        long w = currentSegment.append(c);
        if ( currentSegment.length() > segmentSize) {
            debugIn += currentSegment.close();
        }
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
        System.out.println("buffer pool created: " + pool.getCount() + " capacity: " + pool.getCapacity());
    }

    @Override
    public Chunk read(final Direction dir) throws IOException {
        
        while ( currentSegment == null || !currentSegment.hasMore(dir) ) {
            if ( currentSegment != null ) {
                debugOut += currentSegment.close();
            }
            pool.reclaim();
            
            try {
                if ( dir == Direction.FORWARD ) {
                    if ( position > segments.size() - 1 ) return null;
                    currentSegment = new NIOSegmentImpl(this, segments.get(position++)).openForReading(pool);
                } else {
                    if ( position < 0 ) return null;
                    currentSegment = new NIOSegmentImpl(this, segments.get(position--)).openForReading(pool);
                }
            } catch ( IOException ioe ) {
                if ( currentSegment == null && dir == Direction.REVERSE ) {
//  something bad happened.  Can't even open the header.  but this is the end of the log so move on to the next and see if it will open
                } else {
                    throw ioe;
                }
            }
            
            checkStreamId(currentSegment);
        }        
                
        return currentSegment.next(dir);            
    }
    
    private void checkStreamId(NIOSegmentImpl segment) throws IOException {
        if ( !streamId.equals(segment.getStreamId()) ) {
            throw new IOException(BAD_STREAM_ID);
        }
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
    
    int getSegmentId() {
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
