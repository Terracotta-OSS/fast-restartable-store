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

import com.terracottatech.frs.CipherManager;
import com.terracottatech.frs.RestartStoreException;
import com.terracottatech.frs.SecurePutAction;
import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;
import com.terracottatech.frs.transaction.TransactionManager;

import java.nio.ByteBuffer;

/**
 * @author prasanta
 */
public class SecureCompactorImpl extends CompactorImpl {
  private final CipherManager cipherManager;

  SecureCompactorImpl(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
      TransactionManager transactionManager, ActionManager actionManager, LogManager logManager,
      CompactionPolicy policy, long runIntervalSeconds, long retryIntervalSeconds,
      long compactActionThrottle, int startThreshold, CipherManager cipherManager) {
    super(objectManager, transactionManager, actionManager, logManager, policy, runIntervalSeconds,
        retryIntervalSeconds, compactActionThrottle, startThreshold);
    this.cipherManager = cipherManager;
  }

  public SecureCompactorImpl(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
      TransactionManager transactionManager, LogManager logManager, IOManager ioManager,
      Configuration configuration, ActionManager actionManager, CipherManager cipherManager)
      throws RestartStoreException {
    super(objectManager, transactionManager, logManager, ioManager, configuration, actionManager);
    this.cipherManager = cipherManager;
  }

  @Override
  public CompactionAction convertToAction(
      ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry) {
    ByteBuffer iv = cipherManager.generateInitializationVector();
    ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = getObjectManager();
    SecurePutAction delegate = new SecurePutAction(objectManager, null, cipherManager,
        entry.getId(), entry.getKey(), iv, entry.getValue(), entry.getLsn());
    return new CompactionAction(objectManager, entry, delegate);
  }
}
