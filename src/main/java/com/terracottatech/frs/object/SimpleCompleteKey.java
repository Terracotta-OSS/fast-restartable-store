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
package com.terracottatech.frs.object;

/**
 *
 * @author Chris Dennis
 */
public class SimpleCompleteKey<I, K> implements CompleteKey<I, K> {

  private final I identifer;
  private final K key;

  public SimpleCompleteKey(I identifer, K key) {
    this.identifer = identifer;
    this.key = key;
  }

  @Override
  public I getId() {
    return identifer;
  }

  @Override
  public K getKey() {
    return key;
  }
  
  public boolean equals(Object o) {
    if (o instanceof CompleteKey<?, ?>) {
      CompleteKey<?, ?> key = (CompleteKey<?, ?>) o;
      return getId().equals(key.getId()) && getKey().equals(key.getKey());
    } else {
      return false;
    }
  }
}
