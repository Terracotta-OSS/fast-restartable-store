package com.terracottatech.frs;

import com.terracottatech.frs.config.Configuration;

import java.io.*;
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
public class Backup {
  private static final FilenameFilter SEGMENT_FILENAME_FILTER = new FilenameFilter() {
    @Override
    public boolean accept(File file, String string) {
      return string.startsWith("seg") && string.endsWith(".frs");
    }
  };

  private static final String FRS_BACKUP_LOCKFILE = "frs.backup.lck";
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

    File destBackupLockFile = new File(destFrsFolder, FRS_BACKUP_LOCKFILE);
    destBackupLockFile.createNewFile();

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
    File frsBackupLock = new File(frsFolder, FRS_BACKUP_LOCKFILE);
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
            frsFolder.list(SEGMENT_FILENAME_FILTER)));

    Collections.sort(files);
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

  private static String copyLockString(File sourceFrsFolder) {
    return new File(sourceFrsFolder, FRS_BACKUP_LOCKFILE).getAbsolutePath().intern();
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.out.println("Usage: java com.terracottatech.frs.Backup sourceFolder destFolder");
      System.exit(-1);
    }

    backup(new File(args[0]), new File(args[1]));
  }
}
