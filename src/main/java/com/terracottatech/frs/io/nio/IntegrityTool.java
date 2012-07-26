/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.GCBufferSource;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.log.LogRegionPacker;
import com.terracottatech.frs.log.Signature;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

/**
 *
 * @author mscott
 */
public class IntegrityTool {
    private long lowestLsn = 0;
    private long max = 0;
    private int currentSegment = -1;
    private final File dir;
    

    public IntegrityTool(File f) {
        dir = f;
    }

    public static void main(String[] args) {
        if ( args.length == 0 ) {
            System.err.println("usage: java com.terracottatech.frs.io.nio.IntegrityTool  <<path to stream directory>>");
            return;
        }
        try {
            File dir = new File(args[0]);
            if (!dir.exists()) {
                System.err.println("stream directory does not exist");
                System.exit(99);
            }
            if (!dir.isDirectory()) {
                System.err.println("stream directory is not valid");
                System.exit(99);
            }
            System.err.println("examining " + dir.getAbsolutePath());
            new IntegrityTool(dir).examine();
        } catch (Exception ioe) {
            ioe.printStackTrace();
        }
    }
    
    public void examine() throws Exception {
        NIOSegmentList list = new NIOSegmentList(dir);
        File segfile = list.nextReadFile(Direction.FORWARD);
        while ( segfile != null ) {
            try {
                lowestLsn = examineSegmentFile(segfile);
            } catch ( Exception e ) {
                System.out.println(segfile.getName() + " " + e.getMessage());
            }
            segfile = list.nextReadFile(Direction.FORWARD);
        }
        System.out.format("lowest lsn: %d\n",lowestLsn);        
    }
    
    
    public long examineSegmentFile(File f) throws Exception {
        BufferSource src = new GCBufferSource();
        NIOSegmentImpl segment = new NIOSegmentImpl(null, f);
        segment.openForReading(src);
        int count = 0;
        int size = 0;
        int records = 0;
        int rsize = 0;
        while ( segment.hasMore(Direction.REVERSE) ) {
            Chunk c = segment.next(Direction.REVERSE);
            count++;
            size += c.remaining();
            try {
                List<LogRecord> list = LogRegionPacker.unpack(Signature.ADLER32, c);
                records += list.size();
                for ( LogRecord r : list ) {
                    ByteBuffer[] bb = r.getPayload();
                    for ( ByteBuffer b : bb ) {
                        rsize += b.remaining();
                    }
                }
            } catch ( Exception exp ) {
                System.out.println("!!!!! " + f.getName() + " " + exp.getMessage() + " for chunk " + count + " from the end of size " + size + " !!!!!");
            }
        }
        segment.openForHeader(src);
        boolean closed = segment.last();
        
        int thisSegment = segment.getSegmentId();
        if ( currentSegment >= 0 ) {
            if ( thisSegment != currentSegment + 1) {
                System.out.format("!!!!! Invalid Segment Sequence %d->%d !!!!!\n",currentSegment,thisSegment);
            } 
        }
        currentSegment = thisSegment;
        long thisMin = segment.getBaseMarker();
        long thisMax = segment.getMaximumMarker();
        if ( max > 0 ) {
            if ( thisMin != max + 1 ) {
                System.out.format("!!!!! Invalid LSN Sequence %d->%d  !!!!!\n",max,thisMin);
            }
        }
        max = thisMax;
        System.out.format("%s - min: %d, max: %d, chunks: %d, avg. chunk size: %d, records: %d, avg. record size: %d closed: %b\n",
                f.getName(),thisMin,thisMax,count,(count>0)?size/count:0,records,(records>0)?rsize/records:0,closed);
        return segment.getMinimumMarker();
    }
}
