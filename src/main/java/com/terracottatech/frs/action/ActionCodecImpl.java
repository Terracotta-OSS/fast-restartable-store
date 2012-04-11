/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.action;

import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.util.ByteBufferUtils;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author tim
 */
public class ActionCodecImpl implements ActionCodec {
  private final Map<Class<? extends Action>, ActionID> classToId =
          new HashMap<Class<? extends Action>, ActionID>();
  private final Map<ActionID, Class<? extends Action>> idToClass =
          new HashMap<ActionID, Class<? extends Action>>();
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final ObjectManager<?, ?, ?> objectManager;

  public ActionCodecImpl(ObjectManager<?, ?, ?> objectManager) {
    this.objectManager = objectManager;
  }

  @Override
  public void registerAction(int collectionId, int actionId, Class<? extends Action> actionClass) {
    lock.writeLock().lock();
    try {
      ActionID id = new ActionID(collectionId, actionId);
      if (getConstructor(actionClass) == null)
        throw new IllegalArgumentException("Action class " +
                                                   actionClass +
                                                   " does not have a constructor (ObjectManager, ActionCodec, ByteBuffer[])");
      if (classToId.containsKey(actionClass)) {
        throw new IllegalArgumentException(
                "Action class " + actionClass + " already registered to id " + classToId.get(
                        actionClass));
      }
      if (idToClass.containsKey(id)) {
        throw new IllegalArgumentException(
                "Id " + id + " already registered to action class " + idToClass.get(id));
      }
      classToId.put(actionClass, id);
      idToClass.put(id, actionClass);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public Action decode(ByteBuffer[] buffers) throws ActionDecodeException{
    lock.readLock().lock();
    try {
      ActionID type = ActionID.withByteBuffers(buffers);
      if (!idToClass.containsKey(type))
        throw new IllegalArgumentException("Unknown Action type id= " + type);
      Class<? extends Action> actionClass = idToClass.get(type);
      Constructor<? extends Action> constructor = getConstructor(actionClass);
      try {
        return constructor.newInstance(objectManager, this, buffers);
      } catch (Exception e) {
        throw new ActionDecodeException(e);
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public ByteBuffer[] encode(Action action) {
    ByteBuffer header = headerBuffer(action);
    ByteBuffer[] actionPayload = action.getPayload(this);
    ByteBuffer[] data = new ByteBuffer[actionPayload.length + 1];

    data[0] = header;
    System.arraycopy(actionPayload, 0, data, 1, actionPayload.length);
    return data;
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

  private Constructor<? extends Action> getConstructor(Class<? extends Action> c) {
    try {
      Constructor<? extends Action> constructor = c.getDeclaredConstructor(ObjectManager.class, ActionCodec.class,
                                         ByteBuffer[].class);
      constructor.setAccessible(true);
      return constructor;
    } catch (NoSuchMethodException e) {
      return null;
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
      return new ActionID(ByteBufferUtils.getInt(buffers), ByteBufferUtils.getInt(buffers));
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

      if (action != actionID.action) return false;
      if (collection != actionID.collection) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = collection;
      result = 31 * result + action;
      return result;
    }
  }
}
