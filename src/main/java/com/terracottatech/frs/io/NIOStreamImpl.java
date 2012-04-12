/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
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
    private final File lockFile;
    private final FileLock lock;
    private final FileChannel  lastSync;
    private final boolean      crashed;
    private final long segmentSize;
    private List<File> segments;
    private int position = 0;

    private int lastGoodSegment = Integer.MAX_VALUE;
    private long lastGoodPosition = Long.MAX_VALUE;
    private UUID   streamid;
    
    private static final String format = "seg%09d.frs";
    private static final String segNumFormat = "000000000";
    private static final String BAD_HOME_DIRECTORY = "no home";
    private static final String LOCKFILE_ACTIVE = "lock file exists";
    private static final String BAD_STREAMID = "mis-aligned streams";
    private NIOSegmentImpl currentSegment;
    private final ChunkSource pool = new ChunkSource();

    public NIOStreamImpl(String filepath, long recommendedSize) throws IOException {
        directory = new File(filepath);
        segmentSize = recommendedSize;
        if (!directory.exists() || !directory.isDirectory()) {
            throw new IOException(BAD_HOME_DIRECTORY);
        }
        lockFile = new File(directory, "FRS.lck");
        crashed = !lockFile.createNewFile();
        
        FileOutputStream w = new FileOutputStream(lockFile);
        lastSync = w.getChannel();
        lock = lastSync.tryLock();
        if (lock == null) {
            throw new IOException(LOCKFILE_ACTIVE);
        } else {

        }
        
        enumerateSegments();
    } 
    
    int convertSegmentNumber(File f) {
        try {
            return new DecimalFormat(segNumFormat).parse(f.getName().substring(3,f.getName().length()-4)).intValue();
        } catch ( ParseException pe ) {
            throw new RuntimeException("bad filename",pe);
        }
    }
    
    UUID getStreamId() {
        return streamid;
    }
    
    private void enumerateSegments() throws IOException {        
        ByteBuffer check = ByteBuffer.allocate(28);
        if ( crashed ) {
            while ( check.hasRemaining() ) {
                if ( 0 > lastSync.read(check) ) {
                    throw new IOException("bad log marker");
                }
            }
            check.flip();
            streamid = new UUID(check.getLong(),check.getLong());
            lastGoodSegment = check.getInt();
            lastGoodPosition = check.getLong(); 
        } 
            
        segments = Arrays.asList(
            directory.listFiles(new FilenameFilter() {

                @Override
                public boolean accept(File file, String string) {
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
    
    ChunkSource getChunkSource() {
        return pool;
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
        if ( currentSegment != null ) {
            if ( !currentSegment.isClosed() ) currentSegment.close();
            number = currentSegment.getSegmentNumber() + 1;
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
    public void sync() throws IOException {
        if (currentSegment != null && !currentSegment.isClosed()) {
            long pos = currentSegment.fsync();
            ByteBuffer last = ByteBuffer.allocate(28);
            last.putLong(streamid.getMostSignificantBits());
            last.putLong(streamid.getLeastSignificantBits());
            last.putInt(currentSegment.getSegmentNumber());
            last.putLong(pos);
            last.flip();
            lastSync.position(0);
            while ( last.hasRemaining() ) lastSync.write(last);
            lastSync.force(false);
        }            
        
    }
    //  segment implementation forces before close.  neccessary?

    @Override
    public void close() throws IOException {
        if (currentSegment != null && !currentSegment.isClosed()) {
            currentSegment.close();
        }
        if (lock != null) {
            lock.release();
        }
        if (lockFile != null) {
            lockFile.delete();
        }
    }

    public boolean isClosed() {
        return (lock != null && lock.isValid());
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
        
    }
    
    
}
