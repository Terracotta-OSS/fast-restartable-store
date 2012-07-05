package com.terracottatech.frs.object;

import com.terracottatech.frs.RestartStore;

import java.nio.ByteBuffer;

/**
 * @author tim
 */
public class SimpleRestartableMap extends RestartableMap<String, String, ByteBuffer, ByteBuffer, ByteBuffer> {

  public SimpleRestartableMap(ByteBuffer identifier, RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartability, boolean synchronousWrites) {
    super(identifier, restartability, synchronousWrites);
  }

  @Override
  protected ByteBuffer encodeKey(String key) {
    return ByteBuffer.wrap(key.getBytes());
  }

  @Override
  protected ByteBuffer encodeValue(String value) {
    return ByteBuffer.wrap(value.getBytes());
  }

  @Override
  protected String decodeKey(ByteBuffer rKey) {
    byte[] buf = new byte[rKey.remaining()];
    rKey.slice().get(buf);
    return new String(buf);
  }

  @Override
  protected String decodeValue(ByteBuffer rValue) {
    byte[] buf = new byte[rValue.remaining()];
    rValue.slice().get(buf);
    return new String(buf);
  }

  @Override
  protected long keyByteSize(String key, ByteBuffer encodedKey) {
    return encodedKey.remaining();
  }

  @Override
  protected long valueByteSize(String value, ByteBuffer encodedValue) {
    return encodedValue.remaining();
  }
}
