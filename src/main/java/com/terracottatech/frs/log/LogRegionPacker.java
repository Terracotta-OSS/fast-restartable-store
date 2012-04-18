/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.util.ByteBufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.Adler32;

/**
 *
 * @author mscott
 */
public class LogRegionPacker implements LogRegionFactory<LogRecord> {
    private static final int LOG_RECORD_HEADER_SIZE = ByteBufferUtils.SHORT_SIZE + 3 * ByteBufferUtils.LONG_SIZE;

    //  just hinting
    private int tuningMax = 10;
    private static final short REGION_VERSION = 02;
    private static final byte[] REGION_FORMAT = "NF".getBytes();
    private static final short LR_FORMAT = 02;
    private static final String BAD_CHECKSUM = "bad checksum";
    private final Signature cType;
    
    public LogRegionPacker(Signature sig) {
        cType = sig;
        
        assert(cType == Signature.NONE || cType == Signature.ADLER32);
    }

    @Override
    public Chunk pack(Iterable<LogRecord> payload) {
        return writeRecords(payload);
    }

    @Override
    public List<LogRecord> unpack(Chunk data) throws IOException {
        int headCheck = readHeader(data);
        
        ArrayList<LogRecord> queue = new ArrayList<LogRecord>();
                
        while ( data.hasRemaining() ) {
            queue.add(readRecord(data));
        }
        return queue;
    }

    private Chunk writeRecords(Iterable<LogRecord> records) {        
        ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>(tuningMax);
        long lowestLsn = 0;
        int count = 0;
        
        ByteBuffer header = ByteBuffer.allocate(24);

        buffers.add(header);
        for (LogRecord record : records) {
            ByteBuffer rhead = ByteBuffer.allocate(LOG_RECORD_HEADER_SIZE);
            buffers.add(rhead);

            ByteBuffer[] payload = record.getPayload();
            long len = 0;
            for ( ByteBuffer bb : payload ) {
                len += bb.remaining();
                buffers.add(bb);
                count++;
            }

            if (lowestLsn == 0 || lowestLsn > record.getLowestLsn()) {
                lowestLsn = record.getLowestLsn();
            }

            rhead.putShort(LR_FORMAT);
            rhead.putLong(record.getLsn());
            rhead.putLong(record.getLowestLsn());
            rhead.putLong(len);
            rhead.flip();
        }
        
        formHeader(checksum(buffers.subList(1, buffers.size())),header);
        tuningMax = tuningMax + (int)Math.round((count - tuningMax) * .1);
        return new BufferListWrapper(buffers);
    }
    
    private int readHeader(Chunk data) throws IOException {
        short region = data.getShort();
        long check = data.getLong();
        long check2 = data.getLong();
        byte[] rf = new byte[2];
        data.get(rf);
        
        long checksum = checksum(Arrays.asList(data.getBuffers()));
        
        if ( checksum != check ) throw new IOException(BAD_CHECKSUM);
        return 0;
    }
    
    private int formHeader(long checksum, ByteBuffer header) {
        header.clear();
        header.putShort(REGION_VERSION);
        header.putLong(checksum);
        header.putLong(checksum);
        header.put(REGION_FORMAT);
        header.flip();

        return header.remaining();
    }

    private long checksum(Iterable<ByteBuffer> bufs) {
        if (cType != Signature.ADLER32) return 0;

        Adler32 checksum = new Adler32();
        byte[] temp = null;
        for (ByteBuffer buf : bufs) {
            if (buf.hasArray()) {
                checksum.update(buf.array(),buf.arrayOffset() + buf.position(),buf.arrayOffset() + buf.limit());
            } else {
                if ( temp == null ) temp = new byte[4096];
                buf.mark();
                while (buf.hasRemaining()) {
                    int fetch = ( buf.remaining() > temp.length ) ? temp.length : buf.remaining();
                    buf.get(temp,0,fetch);
                    checksum.update(temp, 0, fetch);
                }
                buf.reset();
            }
        }

        return checksum.getValue();
    }
    
    private LogRecord readRecord(Chunk buffer) {

        short format = buffer.getShort();
        long lsn = buffer.getLong();
        long lLsn = buffer.getLong();
        long len = buffer.getLong();

        ByteBuffer[] payload = buffer.getBuffers(len);
        LogRecord record = new LogRecordImpl(lLsn, payload, null);
        record.updateLsn(lsn);
        return record;
    }
}
