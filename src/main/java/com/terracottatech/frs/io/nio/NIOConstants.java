package com.terracottatech.frs.io.nio;

import java.io.File;
import java.io.FilenameFilter;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.Comparator;

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
