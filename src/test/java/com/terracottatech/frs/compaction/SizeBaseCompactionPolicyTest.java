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
package com.terracottatech.frs.compaction;

import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.IOStatistics;
import com.terracottatech.frs.object.ObjectManager;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SizeBaseCompactionPolicyTest {
  private SizeBasedCompactionPolicy policy;

  @Test
  public void compactionIfOverRatio() throws Exception {
    initialize(0.5, 0.05, 10_000, 100, 30000);
    assertTrue(policy.startCompacting());
  }

  @Test
  public void noCompactionIfUnderRatio() throws Exception {
    initialize(0.5, 0.05, 10_000, 100, 28000);
    assertFalse(policy.startCompacting());
  }

  private void initialize(double threshold, double amount, long objectManagerBytes, long objectManagerSize, long liveSize) throws Exception {
    ObjectManager objectManager = mock(ObjectManager.class);
    when(objectManager.sizeInBytes()).thenReturn(objectManagerBytes);
    when(objectManager.size()).thenReturn(objectManagerSize);

    IOStatistics statistics = mock(IOStatistics.class);
    when(statistics.getLiveSize()).thenReturn(liveSize);
    IOManager ioManager = mock(IOManager.class);
    when(ioManager.getStatistics()).thenReturn(statistics);

    Configuration configuration = mock(Configuration.class);
    when(configuration.getDouble(FrsProperty.COMPACTOR_SIZEBASED_THRESHOLD)).thenReturn(threshold);
    when(configuration.getDouble(FrsProperty.COMPACTOR_SIZEBASED_AMOUNT)).thenReturn(amount);

    policy = new SizeBasedCompactionPolicy(ioManager, objectManager, configuration);
  }
}
