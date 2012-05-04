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
    private ListIterator<File>     readPosition;

    NIOSegmentList(File directory) throws IOException {
        this.directory = directory;
        enumerateSegments();
    }

    private void enumerateSegments() throws IOException {   
        File[] list = directory.listFiles(SEGMENT_FILENAME_FILTER);
        if ( list == null ) list = new File[0];
        segments = Arrays.asList(list);
        
        segments = new LinkedList<File>(segments);
        Collections.sort(segments);        
                
        readPosition = segments.listIterator(segments.size());
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
        if ( segments.isEmpty() ) {
            pos = 0;
        } else if ( pos < 0 || pos > segments.size()-1) {
            pos = segments.size();
        }
        readPosition = segments.listIterator((int)pos);
    }
    
    synchronized File nextReadFile(Direction dir) throws IOException {
        readHead = null;
        if ( dir == Direction.REVERSE ) {
            if ( !readPosition.hasPrevious() ) return null;
            readHead = readPosition.previous();
        } else {
            if ( !readPosition.hasNext() ) return null;
            readHead = readPosition.next();
        }  
        return readHead;
    }
    
    synchronized void removeCurrentSegment() throws IOException {
        readHead.delete();
        readPosition.remove();
    }
    
    synchronized File getEndFile() throws IOException {
        return segments.get(segments.size()-1);
    }
    
    synchronized void removeSegment(int pos) throws IOException {
        File f = segments.remove(pos);
        f.delete();
    }
    
    static int convertSegmentNumber(File f) {
        try {
            return new DecimalFormat(SEG_NUM_FORMAT).parse(f.getName().substring(3, f.getName().length() - 4)).intValue();
        } catch ( ParseException pe ) {
            throw new RuntimeException("bad filename",pe);
        }
    }
}
