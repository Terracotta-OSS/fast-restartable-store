/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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
