/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Direction;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.*;

/**
 *
 * @author mscott
 */
class NIOSegmentList {
    static final FilenameFilter SEGMENT_FILENAME_FILTER = new FilenameFilter() {
      @Override
      public boolean accept(File file, String string) {
        return string.startsWith("seg") && string.endsWith(".frs");
      }
    };

    private static final String SEGMENT_NAME_FORMAT = "seg%09d.frs";
    private static final String SEG_NUM_FORMAT = "000000000";
    
    private List<File>              segments;
    private File                    directory;
    private File                    readHead;
    private File                    writeHead;
    private int                     position;

    NIOSegmentList(File directory) throws IOException {
        this.directory = directory;
        enumerateSegments();
    }

    private synchronized void enumerateSegments() throws IOException {   
        File[] list = directory.listFiles(SEGMENT_FILENAME_FILTER);
        if ( list == null ) list = new File[0];
        segments = Arrays.asList(list);
        
        segments = new LinkedList<File>(segments);
        Collections.sort(segments);        
        
        position = -1;
    }   
    
    synchronized long getTotalSize() {
        long total = 0;
        for ( File f : segments ) {
            total += f.length();
        }
        return total;
    }
    
    synchronized File appendFile() throws IOException {
        int seg = (segments.isEmpty() ) ? 0 : convertSegmentNumber(segments.get(segments.size()-1)) + 1;

        StringBuilder fn = new StringBuilder();
        Formatter pfn = new Formatter(fn);

        pfn.format(SEGMENT_NAME_FORMAT, seg);
        
        writeHead = new File(directory,fn.toString());
        segments.add(writeHead);
        
        return writeHead;
    }
    
    synchronized boolean isEmpty() {
        return segments.isEmpty();
    }
    
    synchronized void setReadPosition(long pos) {
        if ( segments.isEmpty() || pos == 0 ) {
            position = -1;
        } else if ( pos < 0 || pos > segments.size()-1) {
            position = segments.size();
        } else {
            position = (int)pos;
        }
        
    }
    
    synchronized File nextReadFile(Direction dir) throws IOException {
        readHead = null;
        if ( dir == Direction.REVERSE ) {
            position -= 1;
        } else {  //  Direction.FORWARD
            position += 1;
        }  
        if ( position < 0 || position >= segments.size() ) {
            readHead = null;
        } else {
            readHead = segments.get(position);
        }
        
        return readHead;
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
                return size;
            }
            count++;
        }
        assert(segments.get(0).equals(readHead));
        return size;
    }
    
    synchronized long removeFilesFromHead() throws IOException {
        long size = 0;
        while ( position+1 < segments.size()) {
            File f = segments.remove(segments.size()-1);
            size += f.length();
            if ( !f.delete() ) {
                size -= f.length();
                segments.add(f);
                return size;
            }
        }
        assert(readHead == null || segments.get(position).equals(readHead));
        return size;
    }
    
    synchronized void removeCurrentSegment() throws IOException {
        assert(!readHead.equals(writeHead));
        if ( !segments.remove(position).delete() ) {
            segments.add(writeHead);
        }
        readHead = null;
    }
    
    synchronized File getCurrentReadFile() {
        return readHead;
    }
    
    synchronized File getEndFile() throws IOException {
        return segments.get(segments.size()-1);
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
    
    static int convertSegmentNumber(File f) {
        try {
            return new DecimalFormat(SEG_NUM_FORMAT).parse(f.getName().substring(3, f.getName().length() - 4)).intValue();
        } catch ( ParseException pe ) {
            throw new RuntimeException("bad filename",pe);
        }
    }
}
