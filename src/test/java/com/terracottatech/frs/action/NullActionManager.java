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
package com.terracottatech.frs.action;

import com.terracottatech.frs.log.LogRecord;

import java.util.concurrent.Future;

/**
 * @author tim
 */
public class NullActionManager implements ActionManager {
  @Override
  public Future<Void> syncHappened(Action action) {
    return null;
  }

  @Override
  public Future<Void> happened(Action action) {
    return null;
  }

  @Override
  public Action extract(LogRecord record) {
    return null;
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
}
