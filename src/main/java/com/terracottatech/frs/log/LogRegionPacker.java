/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.util.ByteBufferUtils;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.Adler32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mscott
 */
public class LogRegionPacker implements LogRegionFactory<LogRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogManager.class);    
    
    static final int LOG_RECORD_HEADER_SIZE = ByteBufferUtils.SHORT_SIZE + (2 * ByteBufferUtils.LONG_SIZE);
    static final int LOG_REGION_HEADER_SIZE = (2 * ByteBufferUtils.SHORT_SIZE) + (2 * ByteBufferUtils.LONG_SIZE);

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
    
    public static List<LogRecord> unpack(Signature type, Chunk data) throws FormatException {
        long headCheck = readRegionHeader(data);
        
        LinkedList<LogRecord> queue = new LinkedList<LogRecord>();
                
        while ( data.hasRemaining() ) {
            queue.add(readRecord(headCheck,data));
        }
        return queue;
    }
    
     public static List<LogRecord> unpackInReverse(Signature type, Chunk data) throws FormatException {
        long headCheck = readRegionHeader(data);
        
        LinkedList<LogRecord> queue = new LinkedList<LogRecord>();
                
        while ( data.hasRemaining() ) {
            queue.push(readRecord(headCheck,data));
        }
        return queue;
    }   
    

    public List<LogRecord> unpack(Chunk data) throws FormatException {
        long headCheck = readRegionHeader(data);
        
        ArrayList<LogRecord> queue = new ArrayList<LogRecord>();
                
        while ( data.hasRemaining() ) {
            queue.add(readRecord(headCheck,data));
        }
        return queue;
    }

    protected Chunk writeRecords(Iterable<LogRecord> records) {        
        ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>(tuningMax);
        int count = 0;
                
        ByteBuffer headers = ByteBuffer.allocate(512 * 1024);
        ByteBuffer header = headers.duplicate();
        headers.position(LOG_REGION_HEADER_SIZE);

        buffers.add(header);
        for (LogRecord record : records) {
            headers.limit(headers.position() + LOG_RECORD_HEADER_SIZE);
            ByteBuffer rhead = headers.slice();
            headers.position(headers.limit()).limit(headers.position()+LOG_RECORD_HEADER_SIZE);
            
            buffers.add(rhead);

            ByteBuffer[] payload = record.getPayload();
            long len = 0;
            for ( ByteBuffer bb : payload ) {
                len += bb.remaining();
                buffers.add(bb);
                count++;
            }

            formRecordHeader(len,record.getLsn(),rhead);
            rhead.flip();
        }
        
        formRegionHeader(doChecksum() ? checksum(buffers.subList(1, buffers.size())) : 0,header);
        tuningMax = tuningMax + (int)Math.round((count - tuningMax) * .1);
        return new BufferListWrapper(buffers);
    }
    
    protected boolean doChecksum() {
        return cType == Signature.ADLER32;
    }
    
    private static long readRegionHeader(Chunk data) throws FormatException {
        short region = data.getShort();
        long check = data.getLong();
        long check2 = data.getLong();
        byte[] rf = new byte[2];
        data.get(rf);

        if ( check != check2 ) {
            throw new FormatException("log region has mismatched checksums");
        }
        
        if ( region != REGION_VERSION ) {
            throw new FormatException("log region has an unrecognized version code");
        }
        
        if ( check != 0 ) {
            long value = checksum(Arrays.asList(data.getBuffers()));
            if (check != value ) {
                throw new FormatException("Adler32 checksum is not correct",check,value,data.length());
            }
        }
        
        return 0;
    }
    
    protected int formRegionHeader(long checksum, ByteBuffer header) {
        header.clear();
        header.putShort(REGION_VERSION);
        header.putLong(checksum);
        header.putLong(checksum);
        header.put(REGION_FORMAT);
        header.flip();

        return header.remaining();
    }
    
    protected int formRecordHeader(long length, long lsn, ByteBuffer header) {
            header.putShort(LR_FORMAT);
            header.putLong(lsn);
            header.putLong(length);
            return header.remaining();
    }

    protected static long checksum(Iterable<ByteBuffer> bufs) {
        Adler32 checksum = new Adler32();
        byte[] temp = null;
        for (ByteBuffer buf : bufs) {
            if (buf.hasArray()) {
                checksum.update(buf.array(),buf.arrayOffset() + buf.position(),(buf.limit()-buf.position()));
            } else {
                if ( temp == null ) temp = new byte[8192];
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
    
    protected static byte[] md5(Iterable<ByteBuffer> bufs) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            for (ByteBuffer buf : bufs) {
                if (buf.hasArray()) {
                    md5.update(buf.array(),buf.arrayOffset() + buf.position(),(buf.limit()-buf.position()));
                } else {
                    buf.mark();
                    md5.update(buf);
                    buf.reset();
                }
            }

            return md5.digest();
        } catch ( NoSuchAlgorithmException no ) {
            LOGGER.error("MD5 checksumming selected but the package is not available",no);
            return new byte[16];
        }
    }    
    
    private static LogRecord readRecord(long lowestLsn, Chunk buffer) throws FormatException {

        short format = buffer.getShort();
        long lsn = buffer.getLong();
        long len = buffer.getLong();

        if ( format != LR_FORMAT ) throw new FormatException("log record has an unrecognized version code");
        ByteBuffer[] payload = buffer.getBuffers(len);
        LogRecord record = new LogRecordImpl(lowestLsn, payload, null);
        record.updateLsn(lsn);
        return record;
    }
}
