/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.action;

import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.util.ByteBufferUtils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.terracottatech.frs.util.ByteBufferUtils.concatenate;
import static com.terracottatech.frs.util.ByteBufferUtils.getInt;

/**
 * @author tim
 */
public class ActionCodecImpl<I, K, V> implements ActionCodec<I, K, V> {
  private final Map<Class<? extends Action>, ActionID> classToId =
          new HashMap<Class<? extends Action>, ActionID>();
  private final Map<ActionID, ActionFactory<I, K, V>> idToFactory =
          new HashMap<ActionID, ActionFactory<I, K, V>>();
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final ObjectManager<I, K, V> objectManager;

  public ActionCodecImpl(ObjectManager<I, K, V> objectManager) {
    this.objectManager = objectManager;
  }

  @Override
  public void registerAction(int collectionId, int actionId, Class<? extends Action> actionClass,
                             ActionFactory<I, K, V> actionFactory) {
    lock.writeLock().lock();
    try {
      ActionID id = new ActionID(collectionId, actionId);
      if (classToId.containsKey(actionClass)) {
        throw new IllegalArgumentException(
                "Action class " + actionClass + " already registered to id " + classToId.get(
                        actionClass));
      }
      if (idToFactory.containsKey(id)) {
        throw new IllegalArgumentException(
                "Id " + id + " already registered to action class " + idToFactory.get(id));
      }
      classToId.put(actionClass, id);
      idToFactory.put(id, actionFactory);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public Action decode(ByteBuffer[] buffers) {
    lock.readLock().lock();
    try {
      ActionID id = ActionID.withByteBuffers(buffers);
      ActionFactory<I, K, V> factory = idToFactory.get(id);
      if (factory == null)
        throw new IllegalArgumentException("Unknown Action type id= " + id);
      return factory.create(objectManager, this, buffers);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public ByteBuffer[] encode(Action action) {
    return concatenate(headerBuffer(action), action.getPayload(this));
  }

  private ByteBuffer headerBuffer(Action action) {
    lock.readLock().lock();
    try {
      if (!classToId.containsKey(action.getClass()))
        throw new IllegalArgumentException("Unknown action class " + action.getClass());
      return classToId.get(action.getClass()).toByteBuffer();
    } finally {
      lock.readLock().unlock();
    }
  }

  private static class ActionID {
    private final int collection;
    private final int action;

    private ActionID(int collection, int action) {
      this.collection = collection;
      this.action = action;
    }

    static ActionID withByteBuffers(ByteBuffer[] buffers) {
      return new ActionID(getInt(buffers), getInt(buffers));
    }

    ByteBuffer toByteBuffer() {
      ByteBuffer buffer = ByteBuffer.allocate(ByteBufferUtils.INT_SIZE * 2);
      buffer.putInt(collection).putInt(action).flip();
      return buffer;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ActionID actionID = (ActionID) o;

      return action == actionID.action && collection == actionID.collection;
    }

    @Override
    public int hashCode() {
      int result = collection;
      result = 31 * result + action;
      return result;
    }
  }
}
