/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.compaction;


/**
 *
 * @author cdennis
 */
public interface Compactor {

  /**
   * Startup the compactor.
   */
  void startup();

  /**
   * Synchronously shut down the compactor.
   */
  void shutdown() throws InterruptedException;

  /**
   * Callback to notify the compactor that some garbage was generated.
   *
   * @param lsn the LSN of the generated piece of garbage.
   */
  void generatedGarbage(long lsn);

  /**
   * Callback to tell the compactor to run right now, unless it's already running.
   */
  void compactNow();
}
