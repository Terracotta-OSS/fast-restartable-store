/*
 * Copyright (c) 2017-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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
package com.terracottatech.frs.recovery;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.transaction.TransactionActionFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class ConcurrentRecoveryManagerImplTest extends AbstractRecoveryManagerImplTest {
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {{1}, {8}, {72}, {99}, {199}, {256}, {398}, {499}, {508}});
  }

  private final int numProcessors;

  public ConcurrentRecoveryManagerImplTest(int numProcessors) {
    this.numProcessors = numProcessors;
  }

  private TransactionActionFactory transactionActionFactory;
  private LogManager logManager;
  private RecoveryManager recoveryManager;

  @Before
  public void setUp() throws Exception {
    transactionActionFactory = new TransactionActionFactory();
    logManager = newLogManager();
    actionManager = newActionManager();
    recoveryManager = new RecoveryManagerImpl(logManager, actionManager,
        Configuration.getConfiguration(testFolder.newFolder()), numProcessors);
  }

  private Action concurrentAction(boolean shouldReplay, int concurrency) {
    Action mockedAction = action(shouldReplay);
    when(mockedAction.replayConcurrency()).thenReturn(concurrency);
    return mockedAction;
  }

  private Action concurrentAction(long previousLsn, boolean shouldReplay, int concurrency) {
    Action mockedAction = action(previousLsn, shouldReplay);
    when(mockedAction.replayConcurrency()).thenReturn(concurrency);
    return mockedAction;
  }

  @Test
  public void testConcurrentRecover() throws Exception {
    logManager.append(record(8, concurrentAction(false, 1)));
    Action skipper = concurrentAction(8, true, 1);
    logManager.append(record(9, skipper));
    final Random rand = new Random();
    List<Map.Entry<Integer, Action>> transactionalActions = new ArrayList<>();
    IntStream.range(1, 1000).forEach((i) -> {
      try {
        Integer concurrency = rand.nextInt();
        int j = i * 10;
        logManager.append(record(j, concurrentAction(false, concurrency)));
        Action validTransactional = concurrentAction(j, true, concurrency);
        transactionalActions.add(new AbstractMap.SimpleImmutableEntry<>(j + 2, validTransactional));
        logManager.append(record(j + 2, transactionActionFactory.transactionalAction(i, validTransactional, true)));
        logManager.append(record(j + 3, transactionActionFactory.transactionCommit(i)));
      } catch (Exception e) {
        fail("Unexpected Exception");
      }
    });

    logManager.updateLowestLsn(8);
    recoveryManager.recover();

    verify(skipper).replay(9);
    transactionalActions.forEach(entry -> verify(entry.getValue()).replay(entry.getKey()));
  }
}
