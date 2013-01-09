package com.terracottatech.frs;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * A {@link Snapshot} represents a point-in-time state of FRS. In order to save a backup of the state, the files
 * referenced by this snapshot must be copied to another location.
 *
 * @author tim
 */
public interface Snapshot extends Closeable, Iterator<File> {
  /**
   * Release any resources held by this snapshot.
   *
   * @throws IOException
   */
  @Override
  void close() throws IOException;
}
