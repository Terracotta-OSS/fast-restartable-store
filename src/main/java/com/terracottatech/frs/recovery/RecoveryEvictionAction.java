/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.recovery;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionFactory;
import com.terracottatech.frs.action.InvalidatingAction;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.util.ByteBufferUtils;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author tim
 */
class RecoveryEvictionAction implements InvalidatingAction {
  public static final ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer> FACTORY =
          new ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer>() {
            @Override
            public Action create(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                                 ActionCodec codec, ByteBuffer[] buffers) {
              return new RecoveryEvictionAction(ByteBufferUtils.getLongSet(buffers));
            }
          };

  private final Set<Long> invalidatedLsns = new HashSet<Long>();

  RecoveryEvictionAction(Set<Long> invalidatedLsns) {
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
  public Set<Long> replay(long lsn) {
    return Collections.emptySet();
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    if (invalidatedLsns.isEmpty()) {
      return new ByteBuffer[0];
    }
    return new ByteBuffer[] { ByteBufferUtils.serializeLongSet(invalidatedLsns) };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    RecoveryEvictionAction that = (RecoveryEvictionAction) o;

    return invalidatedLsns.equals(that.invalidatedLsns);
  }

  @Override
  public int hashCode() {
    return invalidatedLsns.hashCode();
  }
}
