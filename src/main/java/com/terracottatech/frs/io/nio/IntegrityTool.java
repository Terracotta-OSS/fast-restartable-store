/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.log.LogRegionPacker;
import com.terracottatech.frs.log.Signature;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import static com.terracottatech.frs.config.FrsProperty.FORCE_LOG_REGION_FORMAT;

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
            System.err.println("error processing directory -- " + ioe.getMessage());
        }
    }
    
    public void examine() throws Exception {
        if ( !dir.exists() || !dir.isDirectory() ) {
            throw new IOException("target directory does not exist");
        }
        NIOSegmentList list = new NIOSegmentList(dir);
        
        if ( list.getCount() == 0 ) {
            throw new IOException("no segment files for in the specified directory");
        }
        
        File segfile = list.nextReadFile(Direction.FORWARD);
        while ( segfile != null ) {
            try {
                lowestLsn = examineSegmentFile(segfile);
            } catch ( Exception e ) {
                System.out.println(segfile.getName() + " " + e.getClass().getCanonicalName() + " - " + e.getMessage());
            }
            segfile = list.nextReadFile(Direction.FORWARD);
        }
        System.out.format("lowest lsn: %d\n",lowestLsn);        
    }
    
    
    public long examineSegmentFile(File f) throws Exception {
        if ( dir != null && !f.getParentFile().equals(dir) ) {
            throw new IOException("segment is not part of the current stream");
        }

        WritingSegment segment = new WritingSegment(null, f).open();
        int count = 0;
        int size = 0;
        int records = 0;
        int rsize = 0;
        long lastpos = segment.position();
        Iterator<Chunk> checker = segment.iterator();
        while ( checker.hasNext() ) {
            Chunk c = checker.next();
            count++;
            size += segment.position() - lastpos;
            try {
                List<LogRecord> list = LogRegionPacker.unpack(Signature.ADLER32, (String) FORCE_LOG_REGION_FORMAT.defaultValue(), c);
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
