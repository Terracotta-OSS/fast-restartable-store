/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.log.LogRegion;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author mscott
 */
public class TestLogRegion implements LogRegion {
    
    List<LogRecord> records;

    public TestLogRegion(List<LogRecord> records) {
        this.records = records;
    }
    
    @Override
    public long getLowestLsn() {
        return -1;
    }

    @Override
    public Iterator<LogRecord> iterator() {
        return records.iterator();
    }
    
}
