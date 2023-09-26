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
package com.terracottatech.frs.action;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * @author tim
 */
public class SimpleInvalidatingAction implements InvalidatingAction {
  private final Set<Long> invalidatedLsns = new HashSet<Long>();

  public SimpleInvalidatingAction(Set<Long> invalidatedLsns) {
    this.invalidatedLsns.addAll(invalidatedLsns);
  }

  @Override
  public Set<Long> getInvalidatedLsns() {
    return invalidatedLsns;
  }

  @Override
  public void record(long lsn) {
  }

  @Override
  public void replay(long lsn) {
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    return new ByteBuffer[0];
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SimpleInvalidatingAction that = (SimpleInvalidatingAction) o;

    return invalidatedLsns.equals(that.invalidatedLsns);
  }

  @Override
  public int hashCode() {
    return invalidatedLsns != null ? invalidatedLsns.hashCode() : 0;
  }
}
