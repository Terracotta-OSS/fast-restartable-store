/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import java.nio.ByteBuffer;

/**
 *
 * @author mscott
 */
public class MasterLogRecordFactory implements LogRecordFactory {
    @Override
    public LogRecord createLogRecord(long lowestLsn, ByteBuffer[] payload, LSNEventListener listener) {
        return new LogRecordImpl(lowestLsn, payload, listener);
    }
}
