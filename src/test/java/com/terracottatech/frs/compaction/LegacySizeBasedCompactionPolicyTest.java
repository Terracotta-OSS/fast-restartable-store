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

import org.junit.Test;

import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.IOStatistics;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;

import java.nio.ByteBuffer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author tim
 */
public class LegacySizeBasedCompactionPolicyTest {
  private ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private IOStatistics statistics;
  private LegacySizeBasedCompactionPolicy policy;

  @BeforeClass
  public static void setUpClass() throws Exception {
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  private void createPolicy(double threshold, double amount) throws Exception {
    objectManager = mock(ObjectManager.class);
    statistics = mock(IOStatistics.class);
    IOManager ioManager = mock(IOManager.class);
    when(ioManager.getStatistics()).thenReturn(statistics);
    policy = new LegacySizeBasedCompactionPolicy(ioManager, objectManager, createConfiguration(threshold, amount));
  }

  private Configuration createConfiguration(double threshold, double amount) {
    Configuration configuration = mock(Configuration.class);
    when(configuration.getDouble(FrsProperty.COMPACTOR_SIZEBASED_THRESHOLD)).thenReturn(threshold);
    when(configuration.getDouble(FrsProperty.COMPACTOR_SIZEBASED_AMOUNT)).thenReturn(amount);
    return configuration;
  }

  @Test
  public void testStart() throws Exception {
    createPolicy(0.5, 0.05);
    objectManagerByteSize(10);
    logByteSize(10);
    assertThat(policy.startCompacting(), is(false));
    logByteSize(20);
    assertThat(policy.startCompacting(), is(true));
  }

  @Test(expected = IllegalStateException.class)
  public void testAlreadyStarted() throws Exception {
    createPolicy(0.5, 0.05);
    objectManagerByteSize(1);
    logByteSize(2);
    assertThat(policy.startCompacting(), is(true));
    policy.startCompacting();
  }

  @Test(expected = IllegalStateException.class)
  public void testCompactNotStarted() throws Exception {
    createPolicy(0.5, 0.05);
    policy.compacted(mock(ObjectManagerEntry.class));
  }

  @Test(expected = IllegalStateException.class)
  public void testStopNotStarted() throws Exception {
    createPolicy(0.5, 0.05);
    policy.stoppedCompacting();
  }

  @Test
  public void testCompactAmount() throws Exception {
    createPolicy(0.5, 0.05);
    objectManagerByteSize(100);
    objectManagerSize(100);
    logByteSize(200);
    assertThat(policy.startCompacting(), is(true));
    for (int i = 0; i < 4; i++) {
      assertThat(policy.compacted(mock(ObjectManagerEntry.class)), is(true));
    }
    // tried 5%, but the log size didn't change, so we go again
    assertThat(policy.compacted(mock(ObjectManagerEntry.class)), is(true));

    // Now the log size changes, but we always check at 5% intervals.
    logByteSize(100);
    for (int i = 0; i < 4; i++) {
      assertThat(policy.compacted(mock(ObjectManagerEntry.class)), is(true));
    }
    assertThat(policy.compacted(mock(ObjectManagerEntry.class)), is(false));

  }

  private void objectManagerSize(long count) {
    when(objectManager.size()).thenReturn(count);
  }

  private void objectManagerByteSize(long size) {
    when(objectManager.sizeInBytes()).thenReturn(size);
  }

  private void logByteSize(long size) {
    when(statistics.getLiveSize()).thenReturn(size);
  }
}
