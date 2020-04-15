/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.SnapshotRequest;
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.SimpleBufferSource;
import com.terracottatech.frs.util.ByteBufferUtils;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.zip.Adler32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.terracottatech.frs.PutAction.PUT_ACTION_OVERHEAD;
import static com.terracottatech.frs.action.ActionCodecImpl.ACTION_HEADER_OVERHEAD;
import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 *
 * @author mscott
 */
public class LogRegionPacker implements LogRegionFactory<LogRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogManager.class);    
    
    static final int LOG_RECORD_HEADER_SIZE = ByteBufferUtils.SHORT_SIZE + (2 * ByteBufferUtils.LONG_SIZE);
    static final int LOG_REGION_HEADER_SIZE = (2 * ByteBufferUtils.SHORT_SIZE) + (2 * ByteBufferUtils.LONG_SIZE);

    /* LogRegionPacker.formRecordHeader
    2 bytes - LR_FORMAT
    8 bytes - lsn
    8 bytes - payload length
    */
    private static final long RECORD_HEADER_OVERHEAD = 18;

    private static final long MINIMUM_RECORD_OVERHEAD = RECORD_HEADER_OVERHEAD + ACTION_HEADER_OVERHEAD + PUT_ACTION_OVERHEAD;

    private final BufferSource source;

    //  just hinting
    private int tuningMax = 10;
    static final short REGION_VERSION = 02;
    //we only use the first two US-ASCII bytes of these strings (limited space in the header)
    public static final String OLD_REGION_FORMAT_STRING = "NF";
    public static final String NEW_REGION_FORMAT_STRING = "HT";
    //the old region format bytes sequence (notice the default charset use!)
    static final byte[] OLD_REGION_FORMAT = OLD_REGION_FORMAT_STRING.getBytes();
    static final byte[] NEW_REGION_FORMAT = NEW_REGION_FORMAT_STRING.getBytes(US_ASCII);
    static final short LR_FORMAT = 02;
    private static final String BAD_CHECKSUM = "bad checksum";
    private final Signature cType;
    private final String forcedLogRegionFormat;

    private static final ThreadLocal<byte[]> buffer = new ThreadLocal<byte[]>();
     
    public LogRegionPacker(Signature sig, String forcedLogRegionFormat) {
        this(sig, new SimpleBufferSource(), forcedLogRegionFormat);
    }   
    
    public LogRegionPacker(Signature sig, BufferSource src, String forcedLogRegionFormat) {
        cType = sig;
        
        assert(cType == Signature.NONE || cType == Signature.ADLER32);
        
        this.source = ( src == null ) ? new SimpleBufferSource() : src;
        this.forcedLogRegionFormat = forcedLogRegionFormat;
    }   
    
    @Override
    public Chunk pack(final Iterable<LogRecord> payload) {
       final List<SnapshotRequest> holder = new LinkedList<SnapshotRequest>();
       Chunk c = writeRecords(new Iterable<LogRecord>() {
               @Override
               public Iterator<LogRecord> iterator() {

                   return new Iterator<LogRecord>() {
                       Iterator<LogRecord> delegate = payload.iterator();
                       @Override
                       public boolean hasNext() {
                           return delegate.hasNext();
                       }
    
                       @Override
                       public LogRecord next() {
                           LogRecord lr = delegate.next();
                           if ( lr instanceof SnapshotRequest ) {
                               holder.add((SnapshotRequest)lr);
                           }
                           return lr;
                       }

                       @Override
                       public void remove() {
                           delegate.remove();
                       }
                   };
               }
           });

        if ( !holder.isEmpty() ) {
            return new SnapshotBufferList(Arrays.asList(c.getBuffers()),holder);
        } else {
            return c;
        }
    }
    
    public static LogRecord extract(Signature type, String forcedLogRegionFormat, Chunk data, long match) throws FormatException, IOException {
        long[] spreads = readRegionHeader(forcedLogRegionFormat, data,type == Signature.ADLER32);
        long skip = 0;
        for ( long j : spreads ) {
          long mark = data.getLong(data.position() + skip + j + ByteBufferUtils.SHORT_SIZE);
          if ( mark > match ) {
            break;
          } else {
            skip += j;
          }
        }

        data.skip(skip);
          
        LogRecord target = null;
        while ( target == null ) {
          if ( !data.hasRemaining() ) {
            return null;
          }
          target = readRecord(data,match);
        }
        return target;
    }
    
    public static List<LogRecord> unpack(Signature type, String forcedLogRegionFormat, Chunk data) throws FormatException {
        readRegionHeader(forcedLogRegionFormat, data,type == Signature.ADLER32);
        
        LinkedList<LogRecord> queue = new LinkedList<LogRecord>();
                
        while ( data.hasRemaining() ) {
            queue.add(readRecord(data,-1));
        }
        return queue;
    }
    
     public static List<LogRecord> unpackInReverse(Signature type, String forcedLogRegionFormat, Chunk data) throws FormatException {
        readRegionHeader(forcedLogRegionFormat, data,type == Signature.ADLER32);
        
        LinkedList<LogRecord> queue = new LinkedList<LogRecord>();
                
        while ( data.hasRemaining() ) {
            queue.push(readRecord(data,-1));
        }
        return queue;
    }   
    

    @Override
    public List<LogRecord> unpack(Chunk data) throws FormatException {
        readRegionHeader(forcedLogRegionFormat, data,false);
        
        ArrayList<LogRecord> queue = new ArrayList<LogRecord>();
                
        while ( data.hasRemaining() ) {
            queue.add(readRecord(data,-1));
        }
        return queue;
    }

    protected Chunk writeRecords(Iterable<LogRecord> records) {        
        ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>(tuningMax);
        int count = 0;
        final short HINTS_MAX_SIZE = 256;
        int hintSpread = 16;

        ByteBuffer regionHeader = source.getBuffer(LOG_REGION_HEADER_SIZE);
        ByteBuffer hints = source.getBuffer(((Long.SIZE / Byte.SIZE) * HINTS_MAX_SIZE) + (Short.SIZE / Byte.SIZE));

        buffers.add(regionHeader);
        buffers.add(hints);
        
        ArrayList<Long> spreads = new ArrayList<Long>(HINTS_MAX_SIZE+1);
        long pos = 0;
        for (LogRecord record : records) {
            if ( pos > 0 && count % hintSpread == 0 ) {
              spreads.add(pos);
              pos = 0;
// cull the hints list if it's too big
              if ( spreads.size() > HINTS_MAX_SIZE ) {
                boolean remove = true;
                ListIterator<Long> si = spreads.listIterator();
                while (si.hasNext() ) {
                  pos += si.next();
                  if (remove) {
                    si.remove();
                  } else {
                    si.set(pos);
                    pos = 0;
                  }
                  remove = !remove;
                }
                hintSpread <<= 1;
              }
            }
            ByteBuffer rhead = source.getBuffer(LOG_RECORD_HEADER_SIZE);
            
            buffers.add(rhead);

            ByteBuffer[] payload = record.getPayload();
            long len = 0;
            for ( ByteBuffer bb : payload ) {
                len += bb.remaining();
                buffers.add(bb);
            }
            pos += len;
            pos += LOG_RECORD_HEADER_SIZE;
            count++;
            formRecordHeader(len,record.getLsn(),rhead);
            rhead.flip();
        }
        
        hints.putShort((short)spreads.size());
        for (Long spread : spreads) {
          hints.putLong(spread);
        }
        
        hints.flip();

        formRegionHeader(doChecksum() ? checksum(buffers.subList(2, buffers.size())) : 0,regionHeader);
        tuningMax = tuningMax + (int)Math.round((count - tuningMax) * .1);
        
        return new BufferListWrapper(buffers, source);
    }
    
    protected boolean doChecksum() {
        return cType == Signature.ADLER32;
    }
    
    private static long[] readRegionHeader(String forcedLogRegionFormat, Chunk data, boolean checksum) throws FormatException {
        Chunk header = data.getChunk(ByteBufferUtils.LONG_SIZE * 2 + ByteBufferUtils.SHORT_SIZE + 2);
        try {
            short region = header.getShort();
            long check = header.getLong();
            long check2 = header.getLong();

            byte[] regionFormat;
            if (FrsProperty.FORCE_LOG_REGION_FORMAT.defaultValue().equals(forcedLogRegionFormat)) {
                regionFormat = new byte[2];
                regionFormat[0] = header.get();
                regionFormat[1] = header.get();
            } else {
                regionFormat = Arrays.copyOf(forcedLogRegionFormat.getBytes(US_ASCII), 2);
            }

            if ( check != check2 ) {
                throw new FormatException("log region has mismatched checksums");
            }

            if ( region != REGION_VERSION ) {
                throw new FormatException("log region has an unrecognized version code");
            }

            long[] spreads;
            if (Arrays.equals(NEW_REGION_FORMAT, regionFormat)) {
                spreads = readSpreads(data);
            } else {
                spreads = new long[0];
            }

            if ( check != 0 && checksum ) {
                long value = data.getBuffers() == null ? 
                    checksum(data) : checksum(Arrays.asList(data.getBuffers()));

                if (check != value ) {
                    throw new FormatException("Adler32 checksum is not correct",check,value,data.length());
                }
            }    
            
            return spreads;
        } catch ( IOException ioe ) {
          throw new RuntimeException(ioe);
        } finally {
            if ( header instanceof Closeable ) {
                try {
                    ((Closeable)header).close();
                } catch ( IOException ioe ) {
                    throw new RuntimeException(ioe);
                }
            }
        }
    }
    
    private static long[] readSpreads(Chunk data) throws IOException {
      short len = data.getShort();
      long[] list = new long[len];
      Chunk c = data.getChunk(len * ByteBufferUtils.LONG_SIZE);
      for (int x=0;x<list.length;x++) {
        list[x] = c.getLong();
      }
      if ( c instanceof Closeable ) {
        ((Closeable)c).close();
      }
      return list;
    }
    
    protected int formRegionHeader(long checksum, ByteBuffer header) {
        header.clear();
        header.putShort(REGION_VERSION);
        header.putLong(checksum);
        header.putLong(checksum);
        header.put(NEW_REGION_FORMAT);
        header.flip();

        return header.remaining();
    }
    
    protected int formRecordHeader(long length, long lsn, ByteBuffer header) {
        header.putShort(LR_FORMAT);
        header.putLong(lsn);
        header.putLong(length);
        return header.remaining();
    }
    
    protected static long checksum(Chunk bufs) {
        long pos = bufs.position();
        long lim = bufs.length();
        Adler32 checksum = new Adler32();
        byte[] temp = new byte[8192];
        while (bufs.hasRemaining()) {
            int got = bufs.get(temp);
            checksum.update(temp, 0, got);
        }
        bufs.clear();
        bufs.skip(pos);
        bufs.limit(lim);
        return checksum.getValue();
    }
    
    protected static long checksum(Iterable<ByteBuffer> bufs) {
        Adler32 checksum = new Adler32();
        for (ByteBuffer buf : bufs) {
            if (buf.hasArray()) {
                checksum.update(buf.array(),buf.arrayOffset() + buf.position(),(buf.limit()-buf.position()));
            } else {
                byte[] temp = buffer.get();
                if ( temp == null ) {
                  temp = new byte[8192];
                  buffer.set(temp);
                }                
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
    
    private static LogRecord readRecord(Chunk buffer,long match) throws FormatException {
        Chunk header = buffer.getChunk(ByteBufferUtils.LONG_SIZE * 2 + ByteBufferUtils.SHORT_SIZE);
        long lsn = 0;
        long len = 0;
        try {
            short format = header.getShort();
            lsn = header.getLong();
            len = header.getLong();

            if ( match < 0 || match == lsn ) {
                if ( format != LR_FORMAT ) {
                    throw new FormatException("log record has an unrecognized version code");
                }

                Chunk payload = buffer.getChunk(len);
                LogRecord record = ( payload instanceof Closeable ) ? 
                        new DisposableLogRecordImpl(payload) : 
                        new LogRecordImpl(payload.getBuffers(), null);
                record.updateLsn(lsn);
                return record;
            } else {
                if ( lsn > match ) {
                  throw new AssertionError();
                }
                buffer.skip(len);
                return null;
            } 
        } catch ( Exception exp ) {
          throw new RuntimeException("lsn:" + lsn + " len:" + len + " match:" + match,exp);
        } finally {
            if ( header instanceof Closeable ) {
                try {
                    ((Closeable)header).close();
                } catch ( IOException ioe ) {
                    throw new RuntimeException(ioe);
                }
            }
        }
    }

    public static long getMinimumRecordOverhead() {
        return MINIMUM_RECORD_OVERHEAD;
    }
}
