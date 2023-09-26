/*
 * Copyright (c) 2017-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

public class RestartableMapTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  
  private static final int LONG_BYTES = Long.SIZE / Byte.SIZE;
  
  private TestRestartableMap restartableMap;
  private RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore;

  @Test
  public void testByteSizeWithOverwrites() throws Exception {
    
    File storage = folder.newFolder();

    initialize(storage);
    
    final Long[] SMALL_VALUE = new Long[] { 1L };
    final Long[] BIG_VALUE = new Long[] { 1L, 2L };
    
    // insert some keys and verify size
    final long testMappingsSize = 10;
    for(long i = 0; i < testMappingsSize; i++) {
      restartableMap.put(i, SMALL_VALUE);
    }
    assertThat(restartableMap.getObjectManagerStripe().sizeInBytes(), is((SMALL_VALUE.length + 3) * LONG_BYTES * testMappingsSize));
    
    // verify puts
    for(long i = 0; i < testMappingsSize; i++) {
      assertArrayEquals(restartableMap.get(i), SMALL_VALUE);
    }
    
    //overwrite all keys with big value and verify size
    for(long i = 0; i < testMappingsSize; i++) {
      restartableMap.put(i, BIG_VALUE);
    }
    assertThat(restartableMap.getObjectManagerStripe().sizeInBytes(), is((BIG_VALUE.length + 3) * LONG_BYTES * testMappingsSize));

    //overwrite all keys with small value and verify size
    for(long i = 0; i < testMappingsSize; i++) {
      restartableMap.put(i, SMALL_VALUE);
    }
    assertThat(restartableMap.getObjectManagerStripe().sizeInBytes(), is((SMALL_VALUE.length + 3) * LONG_BYTES * testMappingsSize));
    
    //shutdown & recover
    restartStore.shutdown();
    initialize(storage);

    // test size after recovery
    assertThat(restartableMap.getObjectManagerStripe().sizeInBytes(), is((SMALL_VALUE.length + 3) * LONG_BYTES * testMappingsSize));
    
    //remove all keys and verify size
    for(long i = 0; i < testMappingsSize; i++) {
      restartableMap.remove(i);
    }
    assertThat(restartableMap.getObjectManagerStripe().sizeInBytes(), is(0L));
    
    restartStore.shutdown();
  }
  
  private void initialize(File storage) throws Exception {
    RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>();
    
    restartStore = RestartStoreFactory.createStore(objectManager, storage, new Properties());
    restartableMap = new TestRestartableMap(1, restartStore, true);

    objectManager.registerObject(restartableMap);

    restartStore.startup();
  }

  private static ByteBuffer byteBufferWithLong(long value) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(LONG_BYTES);
    byteBuffer.putLong(value);
    byteBuffer.flip();

    return byteBuffer;
  }
  
  private static class TestRestartableMap extends RestartableMap<Long, Long[], ByteBuffer, ByteBuffer, ByteBuffer> {

    public TestRestartableMap(final int identifier, final RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartability, final boolean synchronousWrites) {
      super(byteBufferWithLong(identifier), restartability, synchronousWrites);
    }

    @Override
    protected ByteBuffer encodeKey(final Long key) {
      return byteBufferWithLong(key);
    }

    @Override
    protected ByteBuffer encodeValue(final Long[] values) {
      int size = values.length;
      ByteBuffer byteBuffer = ByteBuffer.allocate((size + 1) * LONG_BYTES);
      byteBuffer.putLong(size);
      for(int i = 0; i < size; i++) {
        byteBuffer.putLong(values[i]);
      }
      byteBuffer.flip();

      return byteBuffer;
    }

    @Override
    protected Long decodeKey(final ByteBuffer rKey) {
      return rKey.slice().getLong();
    }

    @Override
    protected Long[] decodeValue(final ByteBuffer rValue) {
      ByteBuffer slice = rValue.slice();
      int size = (int)slice.getLong();

      Long[] arr = new Long[size];
      for(int i = 0; i < size; i++) {
        arr[i] = slice.getLong();
      }

      return arr;
    }

    @Override
    protected long keyByteSize(final Long key, final ByteBuffer encodedKey) {
      return encodedKey.remaining();
    }

    @Override
    protected long valueByteSize(final Long[] value, final ByteBuffer encodedValue) {
      return encodedValue.remaining();
    }
  }
}
