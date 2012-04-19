/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.FileBuffer;
import com.terracottatech.frs.io.WrappingChunk;
import static com.terracottatech.frs.util.ByteBufferUtils.SHORT_SIZE;
import static com.terracottatech.frs.util.ByteBufferUtils.INT_SIZE;
import static com.terracottatech.frs.util.ByteBufferUtils.LONG_SIZE;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 *
 * @author mscott
 */
class ReverseReadbackStrategy extends AbstractReadbackStrategy {
    
    private boolean                 initialized = false;
    private final Deque<Long>       fileJumps = new ArrayDeque<Long>();
    private final FileBuffer        buffer;
    protected ListIterator<Chunk>   chunks;
    protected Direction             queueDirection;    
    
    public ReverseReadbackStrategy(FileBuffer buffer) {
        super();
        this.buffer = buffer;
    }

    @Override
    public Iterator<Chunk> iterator() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean hasMore(Direction dir) throws IOException {
        
        if ( dir != Direction.REVERSE ) throw new UnsupportedOperationException("reverse iteration only");
        if ( chunks == null ) queue(dir);
        
        if (  this.chunks.hasNext() ) return true;
        if ( buffer.offset() > fileJumps.getFirst() ) {
            return true;
        }
        return false;
    }

    @Override
    public Chunk iterate(Direction dir) throws IOException {
        if ( dir != Direction.REVERSE ) throw new UnsupportedOperationException("reverse iteration only");
        if ( chunks == null ) queue(dir);
        if ( dir == this.queueDirection && this.chunks.hasNext() ) return this.chunks.next();        
        queue(dir);
        return this.chunks.next();
    }

    public void queue(Direction dir) throws IOException {
        ArrayList<Chunk> list = new ArrayList<Chunk>();
        if ( dir != Direction.REVERSE ) throw new UnsupportedOperationException("reverse iteration only");
        
        if ( !initialized ) {
            buffer.clear();
            buffer.position(-buffer.capacity());
            long read = buffer.read(1);
            assert(read == buffer.length());
            initialized = true;
        } else {
            int front = buffer.getBuffers()[0].remaining();
            assert(buffer.getBuffers()[0].position() == 0);
            int confirm = buffer.getBuffers()[1].getInt(buffer.getBuffers()[1].position()-4);
            assert(SegmentHeaders.FILE_CHUNK.validate(confirm) || SegmentHeaders.CLOSE_FILE.validate(confirm));
            buffer.clear();
            long cp = buffer.offset();
            if ( cp < buffer.length() ) {
                buffer.position(0).limit(cp);
                buffer.partition((int)cp);
            } else {
                buffer.position(cp - buffer.capacity() + (int)front);
                buffer.bufferMove(0, buffer.capacity() - (int)front, front);
                buffer.partition(buffer.capacity() - (int)front);
            }
            buffer.read(1);
        }
        long pos = findStartLocation();
        if ( pos >= 0 ) {
            buffer.partition((int)(pos - buffer.offset()));
            Chunk c = new WrappingChunk(buffer.getBuffers()[1]);
            ByteBuffer[] dest = readChunk(c);
            while ( dest != null ) {
                list.add(new WrappingChunk(dest));
                dest = readChunk(c);
            }
            Collections.reverse(list);
            this.chunks = list.listIterator();
            this.queueDirection = dir;
        }
    }
    
    private long findStartLocation() throws IOException {
        if ( !fileJumps.isEmpty() ) {
            for ( long jump : fileJumps ) {
                if ( jump >= buffer.position() ) return jump;
            }
        }
        
 //  look for close file magic at the end of the buffer
        long pos = buffer.length()-INT_SIZE;
        int cfm = buffer.getInt(pos);
        int fc = 0;
        if ( SegmentHeaders.JUMP_LIST.validate(cfm) ) {
//  good jump list file magic, read a short to see if we have a jump list
            pos -= SHORT_SIZE;
            short jumpCount = buffer.getShort(pos);
            if ( jumpCount > 0 ) {
                pos -= (jumpCount * LONG_SIZE);
                for(int x=0;x<jumpCount;x++) {
                    fileJumps.add(buffer.getLong(pos+(x*LONG_SIZE)));
                }
                if ( SegmentHeaders.CLOSE_FILE.validate(buffer.getInt(pos-INT_SIZE)) ) {
                    for ( long next : fileJumps ) {
                        if ( next >= buffer.position() ) return next;
                    }
                    throw new IOException("buffer is not wide enough");
                }
            }
        } else if ( SegmentHeaders.CLOSE_FILE.validate(cfm) ) {
            pos -= INT_SIZE;
            fc = buffer.getInt(pos);
        } else if ( SegmentHeaders.FILE_CHUNK.validate(cfm) ) {
            fc = cfm;
        }
            
        if ( SegmentHeaders.FILE_CHUNK.validate(fc) ) {
       //  looks like a file chunk, jump to the next
            pos = jumpBackwardChunk(pos-INT_SIZE);
            while ( pos >= 0 ) {
                fileJumps.push(buffer.position()+pos);
                pos = jumpBackwardChunk(pos);
            }
            if ( fileJumps.isEmpty() ) throw new IOException("buffer is not wide enough");
            return fileJumps.getFirst();
        }
 //   brute force
        BitSet fcs = scanFileChunkMagic(buffer);
        int attempt = 0;
        while ( attempt >= 0 ) {
            pos = fcs.nextSetBit(attempt+1);
            long length = buffer.getLong(pos-LONG_SIZE);
            pos -= length;
            pos -= LONG_SIZE;
            if ( pos < 0 ) continue;
            long confirm = buffer.getLong(pos);
            if ( length == confirm ) {
                return buffer.position() + pos;
            }
        }
        
        throw new IOException("buffer is not wide enough");
    }
    
    private long jumpForwardChunk(long magicPos) throws IOException {
        return -1;
    }
    
    private long jumpBackwardChunk(long magicPos) throws IOException {
        if ( magicPos < 0 ) return -1;
        if ( !SegmentHeaders.FILE_CHUNK.validate(buffer.getInt(magicPos)) ) return -1;
        long pos = magicPos - LONG_SIZE;
        if ( pos < 0 ) return -1;
        long length = buffer.getLong(pos);
        pos -= length;
        pos -= LONG_SIZE;
        if ( pos < 0 ) return -1;
        if ( buffer.getLong(pos) == length && SegmentHeaders.CHUNK_START.validate(buffer.getInt(pos-INT_SIZE)) ) {
            return pos-INT_SIZE;
        } else {
            throw new IOException("inconsistent file chunk information");
        }        
    }
    
}
