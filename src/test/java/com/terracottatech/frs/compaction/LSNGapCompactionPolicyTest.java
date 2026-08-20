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

import com.terracottatech.frs.Constants;
import org.junit.Test;

import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;

import java.nio.ByteBuffer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author tim
 */
public class LSNGapCompactionPolicyTest {
  private LSNGapCompactionPolicy policy;
  private ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private LogManager logManager;

  private void createPolicy(int windowSize, double minLoad, double maxLoad) throws Exception {
    Configuration configuration = createConfiguration(windowSize, minLoad, maxLoad);
    objectManager = mock(ObjectManager.class);
    logManager = mock(LogManager.class);
    policy = new LSNGapCompactionPolicy(objectManager, logManager, configuration);
  }

  private Configuration createConfiguration(int windowSize, double minLoad, double maxLoad) {
    Configuration configuration = mock(Configuration.class);
    when(configuration.getInt(FrsProperty.COMPACTOR_LSNGAP_WINDOW_SIZE)).thenReturn(windowSize);
    when(configuration.getDouble(FrsProperty.COMPACTOR_LSNGAP_MIN_LOAD)).thenReturn(minLoad);
    when(configuration.getDouble(FrsProperty.COMPACTOR_LSNGAP_MAX_LOAD)).thenReturn(maxLoad);
    return configuration;
  }

  @Test(expected = IllegalStateException.class)
  public void testStopWithoutStart() throws Exception {
    createPolicy(1, 0.5, 0.8);
    policy.stoppedCompacting();
  }

  @Test(expected = IllegalStateException.class)
  public void testCompactedWithoutStart() throws Exception {
    createPolicy(1, 0.5, 0.8);
    policy.compacted(mock(ObjectManagerEntry.class));
  }

  @Test
  public void testNoGaps() throws Exception {
    createPolicy(1, 0.5, 0.8);
    currentLsn(Constants.FIRST_LSN);
    lowestLsn(0);
    size(100);
    assertThat(policy.startCompacting(), is(false));
  }

  @Test
  public void testLargeEndGap() throws Exception {
    createPolicy(1, 0.5, 0.8);
    currentLsn(Constants.FIRST_LSN);
    lowestLsn(0);
    size(6);
    assertThat(policy.startCompacting(), is(true));
    for (int i = 0; i < 5; i++) {
      lowestLsn(i);
      assertThat(policy.compacted(entry(i)), is(true));
    }
    lowestLsn(Constants.FIRST_LSN);
    assertThat(policy.compacted(entry(99)), is(false));
  }

  @Test
  public void testLargeStartGap() throws Exception {
    createPolicy(1, 0.5, 0.8);
    currentLsn(Constants.FIRST_LSN);
    lowestLsn(0);
    size(6);
    assertThat(policy.startCompacting(), is(true));
    assertThat(policy.compacted(entry(0)), is(true));
    lowestLsn(96);
    assertThat(policy.compacted(entry(95)), is(false));
  }

  @Test
  public void testWindow() throws Exception {
    createPolicy(10, 0.5, 0.8);
    currentLsn(Constants.FIRST_LSN);
    lowestLsn(0);
    size(30);
    assertThat(policy.startCompacting(), is(true));
    lowestLsn(90);
    for (int i = 0; i < 9; i++) {
      assertThat(policy.compacted(entry(80 + i)), is(true));
    }
    assertThat(policy.compacted(entry(10)), is(true));
    for (int i = 0; i < 9; i++) {
      assertThat(policy.compacted(entry(80 + i)), is(true));
    }
    assertThat(policy.compacted(entry(90)), is(false));
  }

  private ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry(long lsn) {
    ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry = mock(ObjectManagerEntry.class);
    when(entry.getLsn()).thenReturn(lsn);
    return entry;
  }

  private void size(long size) {
    when(objectManager.size()).thenReturn(size);
  }

  private void lowestLsn(long lsn) {
    when(objectManager.getLowestLsn()).thenReturn(lsn);
  }

  private void currentLsn(long lsn) {
    when(logManager.currentLsn()).thenReturn(lsn);
  }
}
