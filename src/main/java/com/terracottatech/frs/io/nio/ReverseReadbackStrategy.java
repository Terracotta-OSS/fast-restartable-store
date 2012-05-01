/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.*;
import static com.terracottatech.frs.util.ByteBufferUtils.SHORT_SIZE;
import static com.terracottatech.frs.util.ByteBufferUtils.INT_SIZE;
import static com.terracottatech.frs.util.ByteBufferUtils.LONG_SIZE;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 *
 * @author mscott
 */
class ReverseReadbackStrategy extends AbstractReadbackStrategy {
    
    private boolean                 consistent = false;
    private final Deque<Long>       fileJumps = new ArrayDeque<Long>();
    private final FileChannel        channel;
    private ListIterator<Chunk>   chunks;
    private Direction             queueDirection; 
    private final BufferSource    bufferSrc;
    private ByteBuffer              buffer;
    
    private final int  READ_SIZE = 1024 * 1024;
    
    public ReverseReadbackStrategy(FileChannel channel, BufferSource src) {
        super();
        this.channel = channel;
        bufferSrc = src;
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
        if ( channel.position() > fileJumps.getFirst() ) {
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
        
        if ( buffer == null ) {
            buffer = bufferSrc.getBuffer(READ_SIZE);
            buffer.clear();
            channel.position(-buffer.capacity());
        } else {
            int front = buffer.position();
            int confirm = buffer.getInt(buffer.position()-4);
            assert(SegmentHeaders.FILE_CHUNK.validate(confirm) || SegmentHeaders.CLOSE_FILE.validate(confirm));
            buffer.clear();
            long cp = channel.position();
            ByteBuffer copy = bufferSrc.getBuffer(READ_SIZE);
            if ( cp < copy.capacity() ) {
                copy.limit((int)cp);
                channel.position(0);
            } else {
                copy.limit(front);
                channel.position(cp - buffer.capacity() + front);
            }
            while ( copy.hasRemaining() ) channel.read(copy);
            copy.limit(copy.capacity());
            copy.put(buffer);
            copy.clear();
            buffer = copy;
        }
        int pos = findStartLocation();
        if ( pos >= 0 ) {
            buffer.position(pos);
            buffer.mark();
            Chunk c = new WrappingChunk(buffer);
            ByteBuffer[] dest = readChunk(c);
            while ( dest != null ) {
                list.add(new WrappingChunk(dest));
                dest = readChunk(c);
            }
            buffer.reset();
            Collections.reverse(list);
            this.chunks = list.listIterator();
            this.queueDirection = dir;
        }
    }
    
    private int findStartLocation() throws IOException {
        if ( !fileJumps.isEmpty() ) {
            for ( long jump : fileJumps ) {
                if ( jump >= channel.position() ) return (int)(jump - channel.position());
            }
        }
        
 //  look for close file magic at the end of the buffer
        int pos = buffer.limit()-INT_SIZE;
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
                        if ( next >= channel.position() ) return (int)(next - channel.position());
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
                fileJumps.push(channel.position()+pos);
                pos = jumpBackwardChunk(pos);
            }
            if ( fileJumps.isEmpty() ) throw new IOException("buffer is not wide enough");
            return (int)(fileJumps.getFirst() - channel.position());
        }
 //   brute force
        BitSet fcs = scanFileChunkMagic(new WrappingChunk(buffer));
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
    
    private int jumpBackwardChunk(int magicPos) throws IOException {
        if ( magicPos < 0 ) return -1;
        if ( !SegmentHeaders.FILE_CHUNK.validate(buffer.getInt(magicPos)) ) return -1;
        int pos = magicPos - LONG_SIZE;
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
