/*
 * Copyright (c) 2017-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
