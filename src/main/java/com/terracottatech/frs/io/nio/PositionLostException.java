/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import java.io.IOException;

/**
 * Checked exception thrown when an irrecoverable error (such as a lost position) happens when a fresh channel is
 * created by {@link WrappedFileChannel} due to the loss of the original channel on interrupts.
 */
public class PositionLostException extends IOException {
  public PositionLostException() {
  }
}
