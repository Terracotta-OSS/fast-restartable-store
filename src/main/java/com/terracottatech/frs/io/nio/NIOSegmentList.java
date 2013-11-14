/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Direction;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 *
 * @author mscott
 */
class NIOSegmentList extends AbstractList<File> {
    private final List<File>              segments;
    private final File                    directory;
    private File                          readHead;
    private int                           position;
    private int                           segmentId;
    private long                           cachedTotalSize; 

    NIOSegmentList(File directory) throws IOException {
        this.directory = directory;  
        File[] list = directory.listFiles(NIOConstants.SEGMENT_FILENAME_FILTER);
        if ( list == null ) list = new File[0];
        segments = new LinkedList<File>(Arrays.asList(list));
        Collections.sort(segments, NIOConstants.SEGMENT_FILE_COMPARATOR);
        if ( segments.isEmpty() ) {
          segmentId = 0;
        } else {
          segmentId = NIOConstants.convertSegmentNumber(segments.get(0));
        }
        position = -1;
        for ( int x=0;x<segments.size()-1;x++ ) {
            cachedTotalSize += segments.get(x).length();
        }
    }   
    
    long getTotalSize() {
        return cachedTotalSize;
    }
    
    synchronized File appendFile() throws IOException {
        int seg = segmentId + segments.size();

        StringBuilder fn = new StringBuilder();
        Formatter pfn = new Formatter(fn);

        pfn.format(NIOConstants.SEGMENT_NAME_FORMAT, seg);
        
        File writeHead = new File(directory,fn.toString());
        
        if ( !segments.isEmpty() ) {
          cachedTotalSize += segments.get(segments.size()-1).length();
        }
        
        segments.add(writeHead);
        
        return writeHead;
    }
    
    @Override
    public synchronized boolean isEmpty() {
        return segments.isEmpty();
    }
    
    synchronized void setReadPosition(int pos) {
        if ( segments.isEmpty() || pos == 0 ) {
            position = -1;
        } else if ( pos < 0 || pos > segments.size()-1) {
            position = segmentId + segments.size();
        } else {
            position = (int)pos - segmentId;
        }
        
    }
    
    synchronized File nextReadFile(Direction dir) throws IOException {
        readHead = null;
        if ( dir == Direction.REVERSE ) {
            position -= 1;
        } else {  //  Direction.FORWARD or RANDOM
            position += 1;
        }  
        if ( position < segmentId || position >= segmentId + segments.size() ) {
            readHead = null;
        } else {
            readHead = segments.get(position - segmentId);
        }
        
        return readHead;
    }
    
    int getSegmentPosition() {
      return segmentId + position;
    }
    
    int getBeginningSegmentId() {
      return segmentId;
    }
    
    synchronized long removeFilesFromTail() throws IOException {
        int count = 0;
        long size = 0;
        while ( count < position ) {
            File f = segments.remove(0);
            size += f.length();
            if ( !f.delete() ) {
                size -= f.length();
                segments.add(0,f);
                break;
            }
            count++;
            segmentId++;
        }
        if (!segments.get(0).equals(readHead) || segmentId != NIOConstants.convertSegmentNumber(segments.get(0)) ) {
            throw new AssertionError("bad segment deletion");
        }
        
        cachedTotalSize -= size;
        
        return size;
    }
    
    synchronized long removeFilesFromHead() throws IOException {
        long size = 0;
        while ( position+1 < segmentId + segments.size()) {
            File f = segments.remove(segments.size()-1);
            size += f.length();
            if ( !f.delete() ) {
                size -= f.length();
                segments.add(f);
                break;
            }
        }
        assert(readHead == null || segments.get(position).equals(readHead));
        
        cachedTotalSize -= size;

        return size;
    }
    
    synchronized File getCurrentReadFile() {
        return readHead;
    }
    
    synchronized File getEndFile() throws IOException {
        return segments.get(segments.size()-1);
    }
    
    synchronized File getFile(int segmentId) {
        int spot = segmentId - this.segmentId;
        if ( spot < 0 || spot >= segments.size() ) {
          return null;
        }
        return segments.get(segmentId - this.segmentId);
    }
    
    synchronized boolean currentIsHead() throws IOException {
        if (readHead == null && segments.isEmpty() ) return true;
        if ( readHead != null && segments.isEmpty() ) {
            throw new AssertionError("segment list is inconsistent");
        }
        return segments.get(segments.size()-1).equals(readHead);
    }
    
    synchronized boolean currentIsTail() throws IOException {
        if (readHead == null && segments.isEmpty() ) return true;
        if ( readHead != null && segments.isEmpty() ) {
            throw new AssertionError("segment list is inconsistent");
        }
        return segments.get(0).equals(readHead);
    }    
    
    synchronized File getBeginningFile() throws IOException {
        return segments.get(0);
    }
    
    synchronized int getCount() {
        return segments.size();
    }

    @Override
    public synchronized File get(int i) {
        return segments.get(i);
    }

    @Override
    public synchronized int size() {
        return segments.size();
    }

    @Override
    public synchronized File remove(int i) {
        File f = segments.remove(i);
        
        cachedTotalSize -= f.length();
        
        f.delete();
        
        return f;
    }

    @Override
    public String toString() {
        return "NIOSegmentList{" + "segments=" + segments + ", position=" + position + ", segmentId=" + segmentId + '}';
    }
       
}
