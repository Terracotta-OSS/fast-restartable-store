/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

class FileChannelWriteOpener implements ChannelOpener {
  private final File fileToOpen;
  private FileOutputStream currentStream;
  private volatile boolean closed;

  FileChannelWriteOpener(File fileToOpen) {
    this.fileToOpen = fileToOpen;
    this.currentStream = null;
    this.closed = false;
  }

  synchronized FileChannel open() throws IOException {
    closed = false;
    currentStream = new FileOutputStream(fileToOpen);
    return currentStream.getChannel();
  }

  @Override
  synchronized public FileChannel reopen() throws IOException {
    if (closed) {
      throw new IOException("Channel for file " + fileToOpen.getName() + " cannot be reopened");
    }
    if (currentStream != null) {
      try {
        currentStream.close();
      } catch (IOException ignored) {
      }
    }
    currentStream = new FileOutputStream(fileToOpen);
    return currentStream.getChannel();
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  // explicit close
  @Override
  public synchronized void close() throws IOException {
    closed = true;
    if (currentStream != null) {
      try {
        currentStream.close();
      } catch (IOException ignored) {
      }
    }
    currentStream = null;
  }
}
