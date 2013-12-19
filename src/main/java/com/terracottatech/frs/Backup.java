/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.io.nio.NIOBackupTool;

import java.io.File;
import java.io.IOException;

/**
 * @author tim
 */
public class Backup {
  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: java com.terracottatech.frs.Backup sourceFolder destFolder");
      System.exit(1);
    }
    NIOBackupTool.backup(new File(args[0]), new File(args[1]));
  }
}
