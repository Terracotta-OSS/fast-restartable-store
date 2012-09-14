/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.log.LogRegion;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author mscott
 */
public class DummyLogRegion implements LogRegion {
    
    List<LogRecord> records;

    public DummyLogRegion(List<LogRecord> records) {
        this.records = records;
    }
    
    @Override
    public Iterator<LogRecord> iterator() {
        return records.iterator();
    }
    
}
