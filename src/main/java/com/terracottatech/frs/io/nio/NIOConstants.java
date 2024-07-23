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
package com.terracottatech.frs.io.nio;

import java.io.File;
import java.io.FilenameFilter;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.Comparator;
import java.util.Formatter;

/**
 * @author tim
 */
public abstract class NIOConstants {
  public static final String BACKUP_LOCKFILE = "frs.backup.lck";

  public static final String SEGMENT_NAME_FORMAT = "seg%09d.frs";
  public static final String SEG_NUM_FORMAT = "000000000";

  public static final FilenameFilter SEGMENT_FILENAME_FILTER = new FilenameFilter() {
    @Override
    public boolean accept(File file, String string) {
      return string.startsWith("seg") && string.endsWith(".frs");
    }
  };
  
  public static String convertToSegmentFileName(int seg) {
        StringBuilder fn = new StringBuilder();
        Formatter pfn = new Formatter(fn);

        pfn.format(NIOConstants.SEGMENT_NAME_FORMAT, seg);
        
        return fn.toString();
  }

  public static int convertSegmentNumber(File f) {
    return convertSegmentNumber(f.getName());
  }

  public static int convertSegmentNumber(String name) {
    try {
      return new DecimalFormat(NIOConstants.SEG_NUM_FORMAT).parse(name.substring(3,
                                                                                 name.length() - 4)).intValue();
    } catch ( ParseException pe ) {
      throw new RuntimeException("bad filename",pe);
    }
  }

  public static final Comparator<String> SEGMENT_NAME_COMPARATOR = new Comparator<String>() {
    @Override
    public int compare(String o1, String o2) {
      return convertSegmentNumber(o1) - convertSegmentNumber(o2);
    }
  };

  public static final Comparator<File> SEGMENT_FILE_COMPARATOR = new Comparator<File>() {
    @Override
    public int compare(File o1, File o2) {
      return SEGMENT_NAME_COMPARATOR.compare(o1.getName(), o2.getName());
    }
  };
}
