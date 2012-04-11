/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.Chunk;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author tim
 */
public interface LogRecordFactory {

  LogRecord createLogRecord(long previousLsn, long lowestLsn, ByteBuffer[] payload, LSNEventListener listener);

}
