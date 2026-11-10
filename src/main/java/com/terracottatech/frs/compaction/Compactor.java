/*
 * Copyright IBM Corp. 2024, 2025
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
package com.terracottatech.frs.compaction;


import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 *
 * @author cdennis
 */
public interface Compactor {

  /**
   * Temporarily pause the compactor
   */
  void pause();

  /**
   * Unpause the compactor if paused.
   */
  void unpause();

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

  /**
   * Asynchronously rewrites all live object-manager entries whose LSN is at or
   * below the given LSN, using the supplied executor.
   * <p>
   * The compactor is paused for the duration of the rewrite so that normal
   * background compaction does not interfere. The returned stage completes
   * normally once every qualifying entry has been rewritten, or completes
   * exceptionally if the rewrite fails.
   *
   * @param lsn      the upper-bound LSN up to which entries should
   *                 be rewritten
   * @param executor the executor on which the rewrite work is performed
   * @return a {@link java.util.concurrent.CompletionStage} that completes when
   * the rewrite finishes, or completes exceptionally on failure
   */
  CompletionStage<Void> compactUntilLsn(long lsn, Executor executor);
}
