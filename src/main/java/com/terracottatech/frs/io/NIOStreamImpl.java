/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Formatter;
import java.util.Iterator;

/**
 * NIO implementation of Log Stream.  
 * 
 * @author mscott
 */
public class NIOStreamImpl implements Stream {
    
    File directory;
    File lockFile;
    FileLock lock;
    
    long segmentSize;
    long segNum = 0;
    String format = "seg%09d.frs";
    final String BAD_HOME_DIRECTORY = "no home";
    final String LOCKFILE_ACTIVE = "lock file exists";
    NIOSegmentImpl currentSegment;
    
    public NIOStreamImpl(String filepath, long recommendedSize) throws IOException {
        directory = new File(filepath);
        segmentSize = recommendedSize;
        if ( !directory.exists() || !directory.isDirectory()) throw new IOException(BAD_HOME_DIRECTORY);
        lockFile = new File(directory,"FRS.lck");
        if ( lockFile.createNewFile() ) {

        } else {

        }    
        FileOutputStream w = new FileOutputStream(lockFile);
        lock = w.getChannel().tryLock();
        if ( lock == null ) throw new IOException(this.BAD_HOME_DIRECTORY);
    }
    
    public void shutdown() {
        try {
            close();
        } catch (IOException io ) {
            throw new AssertionError(io);
        }
    }
//  probably doesn't need to be synchronized.  only IO thread should be calling
    @Override
    public synchronized Segment append() throws IOException {
        StringBuilder fn = new StringBuilder();
        Formatter pfn = new Formatter(fn);
        pfn.format(format, segNum++);
        if ( currentSegment != null && !currentSegment.isClosed() ) {
            currentSegment.close();
        }
        currentSegment = new NIOSegmentImpl(new File(directory,fn.toString()),segmentSize,false);
        return currentSegment;
    }
    //  fsync current segment.  old segments are fsyncd on close
    public void sync() throws IOException {
        if ( currentSegment != null && !currentSegment.isClosed() ) currentSegment.fsync();
    }
    //  segment implementation forces before close.  neccessary?
    public void close() throws IOException {
        if ( currentSegment != null && !currentSegment.isClosed() ) {
            currentSegment.close();
            currentSegment = null;
        }
        if ( lock != null ) lock.release();
        if ( lockFile != null ) lockFile.delete();
        lock = null;
        lockFile = null;
        
    }   
    
    public boolean isClose() {
        return lock != null;
    }
    
    @Override
    public Iterator<Segment> iterator(Direction dir) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
