/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Direction;

import java.io.File;
import java.io.IOException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mscott
 */
class NIOSegmentList implements Iterable<File> {
    private final List<File>              segments;
    private final File                    directory;
    private File                          readHead;
    private int                           position;
    private int                           segmentId;
    private long                           cachedTotalSize; 
    private static final Logger LOGGER = LoggerFactory.getLogger(NIOSegmentList.class);

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
        long size = cachedTotalSize;
        synchronized ( this ) {
          if ( !segments.isEmpty() ) {
            size += segments.get(segments.size()-1).length();
          }
        }
        return size;
    }
    
    synchronized File appendFile() throws IOException {
        int seg = segmentId + segments.size();
        
        File writeHead = new File(directory,NIOConstants.convertToSegmentFileName(seg));
        
        if ( !segments.isEmpty() ) {
          cachedTotalSize += segments.get(segments.size()-1).length();
        }
        
        segments.add(writeHead);
        
        return writeHead;
    }
    
    public synchronized boolean isEmpty() {
        return segments.isEmpty();
    }
    
    synchronized void setReadPosition(int pos) {
        if ( segments.isEmpty() || pos == 0 ) {
            position = - 1;
        } else if ( pos < 0 || pos >= segmentId + segments.size()) {
            position = segments.size();
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
        if ( position < 0 || position >= segments.size() ) {
            readHead = null;
        } else {
            readHead = segments.get(position);
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
        }
        segmentId += count;
        position -= count;
        if ((readHead != null && !segments.get(0).equals(readHead)) || segmentId != NIOConstants.convertSegmentNumber(segments.get(0)) ) {
            LOGGER.warn("unable to delete some files during compaction");
        }
        
        cachedTotalSize -= size;
        
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
                break;
            }
        }
        
        if (readHead != null && !segments.get(position).equals(readHead)) {
            LOGGER.warn("unable to delete some files during compaction");
        }
        
        cachedTotalSize -= size;

        return size;
    }
    
    synchronized File getCurrentReadFile() {
      if ( position < 0 || position >= segments.size()) {
        return null;
      }
      if ( readHead == null ) {
        readHead = segments.get(position);
      }
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

    public synchronized File get(int i) {
        return segments.get(i);
    }

    public synchronized int size() {
        return segments.size();
    }

    public synchronized File remove(int i) {
        File f = segments.remove(i);
        
        cachedTotalSize -= f.length();
        
        f.delete();
        
        return f;
    }

    @Override
    public Iterator<File> iterator() {
      return listIterator(0);
    }
    
    public ListIterator<File> listIterator(final int start) {
      return new ListIterator<File>() {
        int position = start;
        @Override
        public boolean hasNext() {
          return position < size();
        }

        @Override
        public File next() {
          if ( position >= size() ) {
            throw new NoSuchElementException();
          }
          return get(position++);
        }

        @Override
        public void remove() {
          NIOSegmentList.this.remove(position);
        }

        @Override
        public boolean hasPrevious() {
          return position > 0;
        }

        @Override
        public File previous() {
          return get(--position);
        }

        @Override
        public int nextIndex() {
          return position + 1;
        }

        @Override
        public int previousIndex() {
          return position - 1;
        }

        @Override
        public void set(File e) {
          throw new UnsupportedOperationException(); 
        }

        @Override
        public void add(File e) {
          throw new UnsupportedOperationException(); 
        }
        
        
      };
    }
    
    public List<File> copyList() {
      return new ArrayList<File>(segments);
    }

    @Override
    public String toString() {
        return "NIOSegmentList{" + "segments=" + segments + ", position=" + position + ", segmentId=" + segmentId + '}';
    }
       
}
