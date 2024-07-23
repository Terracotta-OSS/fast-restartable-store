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
package com.terracottatech.frs.mock.action;

import java.util.concurrent.Future;

import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.object.ObjectManager;

/**
 *
 * @author cdennis
 */
public class MockActionManager implements ActionManager {

  private final ObjectManager<?, ?, ?> objManager;
  private final LogManager logManager;
  
  public MockActionManager(ObjectManager<?, ?, ?> objManager, LogManager logManager) {
    this.objManager = objManager;
    this.logManager = logManager;
  }

  public Future<Void> happened(Action action) {
    LogRecord record = new MockLogRecord(action);
    return logManager.append(record);
  }

  public void asyncHappened(Action action) {
    happened(action);
  }

  public Action extract(LogRecord record) throws IllegalArgumentException {
    if (record instanceof MockLogRecord) {
      Action action = ((MockLogRecord) record).getAction();
      if (action instanceof MockAction) {
        ((MockAction) action).setObjectManager(objManager);
      }
      return action;
    } else {
      throw new AssertionError();
    }
  }

  @Override
  public void pause() {
  }

  @Override
  public void resume() {
  }

  @Override
  public LogRecord barrierAction() {
    return null;
  }

  @Override
  public Future<Void> syncHappened(Action action) {
    return happened(action);
  }
}
