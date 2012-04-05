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
    File lock_file;
    FileLock lock;
    
    long segment_size;
    long seg_num = 0;
    String format = "seg%09d.frs";
    final String BAD_HOME_DIRECTORY = "no home";
    final String LOCK_FILE_ACTIVE = "lock file exists";
    NIOSegmentImpl current_segment;
    
    public NIOStreamImpl(String filepath, long recommended_size) throws IOException {
        directory = new File(filepath);
        segment_size = recommended_size;
        if ( !directory.exists() || !directory.isDirectory()) throw new IOException(BAD_HOME_DIRECTORY);
        lock_file = new File(directory,"FRS.lck");
        if ( lock_file.createNewFile() ) {

        } else {

        }    
        FileOutputStream w = new FileOutputStream(lock_file);
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
        pfn.format(format, seg_num++);
        if ( current_segment != null && !current_segment.isClosed() ) {
            current_segment.close();
        }
        current_segment = new NIOSegmentImpl(new File(directory,fn.toString()),segment_size,false);
        return current_segment;
    }
    //  fsync current segment.  old segments are fsyncd on close
    public void sync() throws IOException {
        if ( current_segment != null && !current_segment.isClosed() ) current_segment.fsync();
    }
    //  segment implementation forces before close.  neccessary?
    public void close() throws IOException {
        if ( current_segment != null && !current_segment.isClosed() ) {
            current_segment.close();
            current_segment = null;
        }
        if ( lock != null ) lock.release();
        if ( lock_file != null ) lock_file.delete();
        lock = null;
        lock_file = null;
        
    }   
    
    public boolean isClose() {
        return lock != null;
    }
    
    @Override
    public Iterator<Segment> iterator(Direction dir) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
