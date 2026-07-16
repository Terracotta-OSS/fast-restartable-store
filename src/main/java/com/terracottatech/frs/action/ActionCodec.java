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
package com.terracottatech.frs.action;

import java.nio.ByteBuffer;

/**
 * @author tim
 */
public interface ActionCodec<I, K, V> {

  default <T extends Action> void registerAndBind(Class<T> actionClass, int collectionId, int actionId,
                                                  ActionSubCodec<I, K, V, ? super T> actionSubCodec) {
    registerCodec(collectionId, actionId, actionSubCodec);
    bind(actionClass, collectionId, actionId);
  }

  <T extends Action> void registerCodec(int collectionId, int actionId, ActionSubCodec<I, K, V, ? super T> actionSubCodec);

  void bind(Class<? extends Action> actionClass, int collectionId, int actionId);

  Action decode(ByteBuffer[] buffer);

  ByteBuffer[] encode(Action action);
}