/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.transaction;

import java.nio.ByteBuffer;

/**
 *
 * @author cdennis
 */
public interface TransactionHandle {
  ByteBuffer toByteBuffer();
}
