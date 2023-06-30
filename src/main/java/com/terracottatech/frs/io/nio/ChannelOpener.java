/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Interface expected by {@link WrappedFileChannel} to reopen a channel and also to check if there was an
 * explicit close invoked on the channel.
 */
interface ChannelOpener extends Closeable {
  FileChannel reopen() throws IOException;
  boolean isClosed();
}