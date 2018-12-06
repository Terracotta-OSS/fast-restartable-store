/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;

class FileChannelReadOpener implements ChannelOpener {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileChannelReadOpener.class);
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
    if (LOGGER.isTraceEnabled()) {
      // For TDB-3758: to check where the premature close is coming from
      LOGGER.trace("Close invoked from ", new Exception());
      final StringBuilder stackTrace = new StringBuilder();
      stackTrace.append("DUMPING ENTIRE STACK").append(System.lineSeparator());
      // dumping stack trace
      Thread.getAllStackTraces().entrySet().stream().map((v) -> {
        stackTrace.append("Thread ID: ").append(v.getKey().getName()).append(" STATE: ").append(v.getKey().getState()).append(System.lineSeparator());
        return Arrays.asList(v.getValue());
      }).forEach((l) -> l.stream().map(StackTraceElement::toString).forEach((s) -> stackTrace.append(s).append(System.lineSeparator())));
      LOGGER.trace(stackTrace.toString());
    }
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