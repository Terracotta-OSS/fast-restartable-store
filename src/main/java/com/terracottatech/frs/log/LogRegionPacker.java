/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.Chunk;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.Adler32;

/**
 *
 * @author mscott
 */
public class LogRegionPacker implements LogRegionFactory<LogRecord> {
    //  just hinting
    private int tuningMax = 10;
    private static final short REGION_VERSION = 02;
    private static final byte[] REGION_FORMAT = "NF".getBytes();
    private static final short LR_FORMAT = 02;
    private static final Adler32 checksum = new Adler32();
    private final Signature cType;
    private final LogRecordFactory recordFactory;
    
    public LogRegionPacker(LogRecordFactory recordFactory, Signature sig) {
        cType = sig;
        
        assert(cType == sig.NONE || cType == sig.ADLER32);
        
        this.recordFactory = recordFactory;
    }

    @Override
    public Chunk pack(Iterable<LogRecord> payload) {
        return convert(payload);
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

    private Chunk convert(Iterable<LogRecord> records) {        
        ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>(tuningMax);
        long lowestLsn = 0;
        
        ByteBuffer header = ByteBuffer.allocate(24);

        buffers.add(header);
        for (LogRecord record : records) {
            ByteBuffer rhead = ByteBuffer.allocate(34);
            buffers.add(rhead);

            ByteBuffer[] payload = record.getPayload();
            long len = checksumAndAdd(buffers, payload);
            if (lowestLsn == 0 || lowestLsn > record.getLowestLsn()) {
                lowestLsn = record.getLowestLsn();
            }

            rhead.putShort(LR_FORMAT);
            rhead.putLong(record.getLsn());
            rhead.putLong(record.getPreviousLsn());
            rhead.putLong(record.getLowestLsn());
            rhead.putLong(len);
            rhead.flip();
        }
        
        formHeader(header);

        return new BufferListWrapper(buffers);
    }
    
    private int readHeader(Chunk data) throws IOException {
        ByteBuffer header = data.getBuffers()[0];
        short region = header.getShort();
        long check = header.getLong();
        long check2 = header.getLong();
        byte[] rf = new byte[2];
        header.get(rf);
        return 0;
    }
    
    private int formHeader(ByteBuffer header) {
        header.clear();
        header.putShort(REGION_VERSION);
        if (cType == Signature.ADLER32) {
            header.putLong(checksum.getValue());
            header.putLong(checksum.getValue());
        } else {
            header.putLong(0x00l);
            header.putLong(0x00l);
        }
        header.put(REGION_FORMAT);
        header.flip();

        return header.remaining();
    }

    private long checksumAndAdd(List<ByteBuffer> list, ByteBuffer[] bufs) {
        long len = 0;

        for (ByteBuffer buf : bufs) {
            if (cType == Signature.ADLER32) {
                if (buf.hasArray()) {
                    checksum.update(buf.array());
                } else {
                    byte[] temp = new byte[4096];
                    while (buf.hasRemaining()) {
                        int pos = buf.position();
                        buf.get(temp);
                        checksum.update(temp, 0, buf.position() - pos);
                    }
                    buf.flip();
                }
            }
            len += buf.remaining();
            list.add(buf);
        }

        return len;
    }
    
    private LogRecord readRecord(Chunk buffer) {

        short format = buffer.getShort();
        long lsn = buffer.getLong();
        long pLsn = buffer.getLong();
        long lLsn = buffer.getLong();
        long len = buffer.getLong();

        ByteBuffer payload = buffer.getBuffer((int)len);
        return new LogRecordImpl(lsn, pLsn, new ByteBuffer[]{payload}, null);
    }
}
