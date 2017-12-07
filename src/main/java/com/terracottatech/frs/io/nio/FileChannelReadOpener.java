/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

class FileChannelReadOpener implements ChannelOpener {
  private final File fileToOpen;
  private FileInputStream currentStream;
  private volatile boolean closed;

  FileChannelReadOpener(File fileToOpen) {
    this.fileToOpen = fileToOpen;
    this.currentStream = null;
    this.closed = false;
  }

  synchronized FileChannel open() throws IOException {
    closed = false;
    currentStream = new FileInputStream(fileToOpen);
    return currentStream.getChannel();
  }

  @Override
  public synchronized FileChannel reopen() throws IOException {
    if (closed) {
      throw new IOException("Channel for file " + fileToOpen.getName() + " cannot be reopened");
    }
    if (currentStream != null) {
      try {
        currentStream.close();
      } catch (IOException ignored) {
      }
    }
    currentStream = new FileInputStream(fileToOpen);
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