/*
 * Copyright (c) 2012-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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
package com.terracottatech.frs.mock.object;

import com.terracottatech.frs.object.CompleteKey;

/**
 *
 * @author cdennis
 */
public class MockCompleteKey<I, K> implements CompleteKey<I, K> {

  private final I id;
  private final K key;
  
  public MockCompleteKey(I id, K key) {
    this.id = id;
    this.key = key;
  }
  
  @Override
  public I getId() {
    return id;
  }

  @Override
  public K getKey() {
    return key;
  }
  
}
