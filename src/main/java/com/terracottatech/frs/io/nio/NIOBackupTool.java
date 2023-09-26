/*
 * Copyright (c) 2012-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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

import com.terracottatech.frs.config.Configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author tim
 */
public class NIOBackupTool {
  private static final int MAX_LOCK_RETRIES = 100;

  public static void backup(File sourceFrsFolder, File destFrsFolder) throws IOException {
    if (!sourceFrsFolder.isDirectory()) {
      throw new IOException(sourceFrsFolder + " is not a directory.");
    }

    if (!destFrsFolder.mkdirs()) {
      throw new IOException("Failed to create the destination directory " + destFrsFolder);
    }

    FileLock lock = lockFrsFolder(sourceFrsFolder);

    copyConfiguration(sourceFrsFolder, destFrsFolder);

    copyDataFiles(sourceFrsFolder, destFrsFolder);

    File destBackupLockFile = new File(destFrsFolder, NIOConstants.BACKUP_LOCKFILE);
    if (!destBackupLockFile.createNewFile()) {
      System.err.println("Unabled to create backup lockfile.");
    }

    lock.release();
  }

  private static void copyDataFiles(File sourceFrsFolder, File destFrsFolder) throws IOException {
    synchronized (copyLockString(sourceFrsFolder)) {
      for (String file : listSortedFiles(sourceFrsFolder)) {
        File sourceFile = new File(sourceFrsFolder, file);
        File destFile = new File(destFrsFolder, file);
        copyFile(sourceFile, destFile);
      }
    }
  }

  private static FileLock lockFrsFolder(File frsFolder) throws IOException {
    File frsBackupLock = new File(frsFolder, NIOConstants.BACKUP_LOCKFILE);
    if (!frsBackupLock.exists() || !frsBackupLock.isFile()) {
      throw new IOException("Missing backup lockfile.");
    }

    FileInputStream fis = new FileInputStream(frsBackupLock);
    FileChannel channel = fis.getChannel();

    for (int i = 0; i < MAX_LOCK_RETRIES; i++) {
      FileLock lock;
      try {
        lock = channel.lock(0, Long.MAX_VALUE, true);
      } catch (OverlappingFileLockException e) {
        // We're trying to lock from the same JVM, just pretend the lock acquisition failed
        // and retry later.
        lock = null;
      }
      if (lock != null) {
        return lock;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    throw new IOException("Failed to lock data folder " + frsFolder);
  }

  private static List<String> listSortedFiles(File frsFolder) {
    List<String> files = new ArrayList<String>(Arrays.asList(
            frsFolder.list(NIOConstants.SEGMENT_FILENAME_FILTER)));

    Collections.sort(files, NIOConstants.SEGMENT_NAME_COMPARATOR);
    return files;
  }

  private static void copyConfiguration(File sourceFrsFolder, File destFrsFolder) throws IOException {
    File sourceConfiguration = new File(sourceFrsFolder,
                                        Configuration.USER_PROPERTIES_FILE);
    if (sourceConfiguration.exists() && sourceConfiguration.isFile()) {
      copyFile(sourceConfiguration, new File(destFrsFolder, Configuration.USER_PROPERTIES_FILE));
    }
  }

  private static void copyFile(File sourceFile, File destFile) throws IOException {
    if (!sourceFile.exists() || !sourceFile.isFile()) {
      throw new IOException("Source " + sourceFile + " exists=" + sourceFile.exists() + " isFile=" + sourceFile.isFile());
    }

    FileOutputStream fos = new FileOutputStream(destFile);
    FileInputStream fis = new FileInputStream(sourceFile);
    FileChannel output = fos.getChannel();
    FileChannel input = fis.getChannel();
    try {
      long size = input.size();
      long position = 0;

      while (position < size) {
        position += output.transferFrom(input, position, size);
      }
    } finally {
      fos.close();
      fis.close();
    }
  }

  private static String copyLockString(File sourceFrsFolder) throws IOException {
    return new File(sourceFrsFolder, NIOConstants.BACKUP_LOCKFILE).getCanonicalPath().intern();
  }
}
