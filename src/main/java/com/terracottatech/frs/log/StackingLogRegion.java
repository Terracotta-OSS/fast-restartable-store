/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.Chunk;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.Adler32;

/**
 *
 * @author mscott
 */
public class StackingLogRegion implements LogRegion, Chunk {

    static final short REGION_VERSION = 02;
    static final byte[] REGION_FORMAT = "NF".getBytes();
    static final short LR_FORMAT = 02;
    Adler32 checksum = new Adler32();
// set at getBuffers
    final LogRecord[] regions;
    private long totalLen = 0;
    private long lowestLsn = 0;
// set at construction    
    private boolean dochecksum = false;
    private final long baseLsn;
//  half synchronized
    private volatile boolean syncing = false;
//  these are synchronized     
    private long endLsn;
    private boolean closed = false;
    private boolean written = false;
    private int count = 0;
    
    private Object guard = new Object();
    StackingLogRegion next;

    public StackingLogRegion(boolean use_checksum, long startLsn, int max_size) {
        baseLsn = startLsn;
        endLsn = startLsn-1;
        regions = new LogRecord[max_size];
        this.dochecksum = use_checksum;
    }

    public boolean append(LogRecord record) {
        assert (record.getLsn() >= baseLsn);

        if (record.getLsn() >= regions.length + baseLsn) {
            return false;
        }

        regions[(int) (record.getLsn() - baseLsn)] = record;

        if (!countRecord(record.getLsn())) {
            regions[(int) (record.getLsn() - baseLsn)] = null; //  just to be clean;
            return false;
        }

        return true;
    }

    /**
     * this method is only called by the IO thread.
     *
     * @return
     */
    StackingLogRegion next() {
        assert (closed);
        if (next != null) {
            return next;
        }
        synchronized (guard) {
            if (next == null) {
                next = new StackingLogRegion(dochecksum, endLsn + 1, regions.length);
            }
        }
        return next;
    }

    long getBaseLsn() {
        return baseLsn;
    }

    long getEndLsn() {
        return endLsn;
    }
    //  TODO:  make more concurrent
    private synchronized boolean countRecord(long lsn) {
        if (closed) {
            if (lsn > endLsn) {
                return false;
            }
            if (count == endLsn - baseLsn) {
                this.notify();  // adding one will make count match slots
            } else if (count >= endLsn - baseLsn) {
                throw new AssertionError();  // too many, something bad happend
            }
        } else if (lsn > endLsn) {
            endLsn = lsn;
        }
        count += 1;

        return true;
    }

    public synchronized boolean close(boolean sync) {
        closed = true;
        syncing = sync;

        this.notify();
        return baseLsn + count == endLsn;
    }

    //  can this slip due to out of order if its not synchronized?

    public boolean isSyncing() {
        return syncing;
    }

    public void waitForWrite() throws InterruptedException {
        synchronized (guard) {
            while (!this.written) {
                guard.wait();
            }
        }
    }

    public void waitForWrite(long millis) throws InterruptedException {
        long span = System.currentTimeMillis();
        synchronized (guard) {
            while (!this.written) {
                if (System.currentTimeMillis() - span > millis) {
                    return;
                }
                guard.wait(millis);
            }
        }
    }

    public boolean isWritten() {
        synchronized (guard) {
            return written;
        }
    }

    public void written() {
        synchronized (guard) {
            written = true;
            guard.notifyAll();
        }
    }

    public synchronized void waitForContiguous() throws InterruptedException {
        while (!closed || count != endLsn - baseLsn + 1) {
            this.wait();
        }
    }

    @Override
    public long getLowestLsn() {
        return lowestLsn;
    }

    @Override
    public ByteBuffer[] getBuffers() {
        if (!closed || count != endLsn - baseLsn + 1) {
            throw new AssertionError();
        }

        ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
        ByteBuffer header = ByteBuffer.allocate(24);

        buffers.add(header);
        for (int x = 0; x < count; x++) {
            ByteBuffer rhead = ByteBuffer.allocate(34);
            buffers.add(rhead);

            ByteBuffer[] record = regions[x].getPayload();
            long len = checksum(buffers, record);
            totalLen += len;
            if (lowestLsn == 0 || lowestLsn > regions[x].getLowestLsn()) {
                lowestLsn = regions[x].getLowestLsn();
            }

            rhead.putShort(LR_FORMAT);
            rhead.putLong(regions[x].getLsn());
            rhead.putLong(regions[x].getPreviousLsn());
            rhead.putLong(regions[x].getLowestLsn());
            rhead.putLong(len);
            rhead.flip();
        }

        totalLen = formHeader(header);

        return buffers.toArray(new ByteBuffer[buffers.size()]);
    }

    private int formHeader(ByteBuffer header) {
        header.clear();
        header.putShort(REGION_VERSION);
        if (dochecksum) {
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

    private long checksum(List<ByteBuffer> list, ByteBuffer[] bufs) {
        long len = 0;

        for (ByteBuffer buf : bufs) {
            if (dochecksum) {
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

    @Override
    public long length() {
        return totalLen;
    }
}
