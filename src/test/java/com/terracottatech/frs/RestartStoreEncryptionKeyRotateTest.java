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
package com.terracottatech.frs;

import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.object.RegisterableObjectManager;
import com.terracottatech.frs.object.SimpleRestartableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static com.terracottatech.frs.cipher.EncryptionManagerImpl.MULTIPLE_TOKEN_KEY_DELIMETER;
import static com.terracottatech.frs.cipher.EncryptionManagerImpl.TOKEN_KEY_DELIMITER;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class RestartStoreEncryptionKeyRotateTest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private Properties properties = new Properties();

  @Before
  public void setUp() {
    properties = CipherHelper.configure(true, properties);
    properties.setProperty(FrsProperty.IO_NIO_SEGMENT_SIZE.shortName(), "1000000");
  }

  @Test
  public void testRecordUpdatedWithNewEncKey() throws Exception {
    RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<>();
    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
        RestartStoreFactory.createStore(objectManager, folder.newFolder(), properties);

    CountDownLatch latch = new CountDownLatch(1);

    restartStore.registerEncCompletionListener((event) -> {
      latch.countDown();
    });

    restartStore.startup().get();
    Map<String, String> map1 = createMap(restartStore, objectManager, 0);
    Map<String, String> map2 = createMap(restartStore, objectManager, 1);
    for (int i = 0; i < 10000; ++i) {
      map1.put(String.valueOf(i), "val" + i);
      map2.put(String.valueOf(i), "val" + i);
    }

    restartStore.handleEncKeyChange("token2", CipherHelper.generateNewKey());

    // Get and Put should still work
    for (int i = 5000; i < 10000; ++i) {
      assertThat(map1.get(String.valueOf(i)), is("val" + i));
      assertThat(map2.get(String.valueOf(i)), is("val" + i));
    }

    for (int i = 10000; i < 20000; ++i) {
      map1.put(String.valueOf(i), "val" + i);
      map2.put(String.valueOf(i), "val" + i);
    }
    latch.await();

    for (int i = 0; i < 20000; ++i) {
      assertThat(map1.get(String.valueOf(i)), is("val" + i));
      assertThat(map2.get(String.valueOf(i)), is("val" + i));
    }
    assertThat(restartStore.isUsingEncKey("token1"), is(false));
    assertThat(restartStore.isUsingEncKey("token2"), is(true));

    restartStore.shutdown();
  }

  @Test
  public void testRecordUpdatedWithNewEncKeyDuringPauseAndResume() throws Exception {
    String newKey = "";
    File path = folder.newFolder();
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<>();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
          RestartStoreFactory.createStore(objectManager, path, properties);

      CountDownLatch latch = new CountDownLatch(1);

      restartStore.registerEncCompletionListener(event -> latch.countDown());

      restartStore.startup().get();
      Map<String, String> map1 = createMap(restartStore, objectManager, 0);
      Map<String, String> map2 = createMap(restartStore, objectManager, 1);
      for (int i = 0; i < 10000; ++i) {
        map1.put(String.valueOf(i), "val" + i);
        map2.put(String.valueOf(i), "val" + i);
      }
      newKey = CipherHelper.generateNewKey();
      restartStore.handleEncKeyChange("token2", newKey);
      Future<Future<Snapshot>> snapshotFutureSquare = restartStore.pause();
      Future<Snapshot> snapshotFuture = snapshotFutureSquare.get();
      try {
        restartStore.resume();
      } catch (NotPausedException e) {

      }

      //  Get and Put should still work
      for (int i = 5000; i < 10000; ++i) {
        assertThat(map1.get(String.valueOf(i)), is("val" + i));
        assertThat(map2.get(String.valueOf(i)), is("val" + i));
      }

      for (int i = 10000; i < 20000; ++i) {
        map1.put(String.valueOf(i), "val" + i);
        map2.put(String.valueOf(i), "val" + i);
      }
      snapshotFuture.get().close();
      latch.await();

      for (int i = 0; i < 20000; ++i) {
        assertThat(map1.get(String.valueOf(i)), is("val" + i));
        assertThat(map2.get(String.valueOf(i)), is("val" + i));
      }
      assertThat(restartStore.isUsingEncKey("token1"), is(false));
      assertThat(restartStore.isUsingEncKey("token2"), is(true));

      restartStore.shutdown();
    }

    removeOldTokenAndKey();
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<>();
      properties.setProperty(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY.shortName(), "token2" + TOKEN_KEY_DELIMITER + newKey);

      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
          RestartStoreFactory.createStore(objectManager, path, properties);

      Map<String, String> map1 = createMap(restartStore, objectManager, 0);
      Map<String, String> map2 = createMap(restartStore, objectManager, 1);
      restartStore.startup().get();
      for (int i = 0; i < 20000; ++i) {
        assertThat(map1.get(String.valueOf(i)), is("val" + i));
        assertThat(map2.get(String.valueOf(i)), is("val" + i));
      }
      restartStore.shutdown();
    }
  }

  @Test
  public void testRecordUpdatePartiallyAndFinishAfterRecovering() throws Exception {
    String newKey = "";
    String oldTokenAndKey = "";
    File path = folder.newFolder();
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<>();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
          RestartStoreFactory.createStore(objectManager, path, properties);

      restartStore.startup().get();
      Map<String, String> map1 = createMap(restartStore, objectManager, 0);
      Map<String, String> map2 = createMap(restartStore, objectManager, 1);
      for (int i = 0; i < 20000; ++i) {
        map1.put(String.valueOf(i), "val" + i);
        map2.put(String.valueOf(i), "val" + i);
      }
      oldTokenAndKey = properties.getProperty(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY.shortName());
      newKey = CipherHelper.generateNewKey();
      restartStore.handleEncKeyChange("token2", newKey);
      restartStore.shutdown();
    }

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<>();
      properties.setProperty(FrsProperty.STORE_ENCRYPTION_OLD_TOKENS_AND_KEYS.shortName(), oldTokenAndKey);
      properties.setProperty(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY.shortName(), "token2" + TOKEN_KEY_DELIMITER + newKey);

      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
          RestartStoreFactory.createStore(objectManager, path, properties);

      Map<String, String> map1 = createMap(restartStore, objectManager, 0);
      Map<String, String> map2 = createMap(restartStore, objectManager, 1);
      restartStore.startup().get();
      for (int i = 0; i < 20000; ++i) {
        assertThat(map1.get(String.valueOf(i)), is("val" + i));
        assertThat(map2.get(String.valueOf(i)), is("val" + i));
      }
      restartStore.shutdown();
    }

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<>();
      properties.setProperty(FrsProperty.STORE_ENCRYPTION_OLD_TOKENS_AND_KEYS.shortName(), oldTokenAndKey);
      properties.setProperty(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY.shortName(), "token2" + TOKEN_KEY_DELIMITER + newKey);

      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
          RestartStoreFactory.createStore(objectManager, path, properties);

      CountDownLatch latch = new CountDownLatch(1);
      restartStore.registerEncCompletionListener(event -> latch.countDown());
      createMap(restartStore, objectManager, 0);
      createMap(restartStore, objectManager, 1);
      restartStore.startup().get();
      latch.await();
      assertThat(restartStore.isUsingEncKey("token1"), is(false));
      assertThat(restartStore.isUsingEncKey("token2"), is(true));
      restartStore.shutdown();
    }

    removeOldTokenAndKey();
    String latestKey = "";
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<>();
      properties.setProperty(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY.shortName(), "token2" + TOKEN_KEY_DELIMITER + newKey);

      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
          RestartStoreFactory.createStore(objectManager, path, properties);

      Map<String, String> map1 = createMap(restartStore, objectManager, 0);
      Map<String, String> map2 = createMap(restartStore, objectManager, 1);
      restartStore.startup().get();

      for (int i = 20000; i < 40000; i++) {
        map1.put(String.valueOf(i), "val" + i);
        map2.put(String.valueOf(i), "val" + i);
      }
      latestKey = CipherHelper.generateNewKey();
      restartStore.handleEncKeyChange("token3", latestKey);
      Thread.sleep(100);
      restartStore.shutdown();
    }

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<>();
      properties.setProperty(FrsProperty.STORE_ENCRYPTION_OLD_TOKENS_AND_KEYS.shortName(), "token2" + TOKEN_KEY_DELIMITER + newKey);
      properties.setProperty(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY.shortName(), "token3" + TOKEN_KEY_DELIMITER + latestKey);

      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
          RestartStoreFactory.createStore(objectManager, path, properties);

      createMap(restartStore, objectManager, 0);
      createMap(restartStore, objectManager, 1);
      restartStore.startup().get();
      restartStore.shutdown();
    }

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<>();
      properties.setProperty(FrsProperty.STORE_ENCRYPTION_OLD_TOKENS_AND_KEYS.shortName(), "token2" + TOKEN_KEY_DELIMITER + newKey);
      properties.setProperty(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY.shortName(), "token3" + TOKEN_KEY_DELIMITER + latestKey);

      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
          RestartStoreFactory.createStore(objectManager, path, properties);

      CountDownLatch latch = new CountDownLatch(1);
      restartStore.registerEncCompletionListener(event -> latch.countDown());
      Map<String, String> map1 = createMap(restartStore, objectManager, 0);
      Map<String, String> map2 = createMap(restartStore, objectManager, 1);
      restartStore.startup().get();
      latch.await();
      for (int i = 0; i < 40000; ++i) {
        assertThat(map1.get(String.valueOf(i)), is("val" + i));
        assertThat(map2.get(String.valueOf(i)), is("val" + i));
      }
      assertThat(restartStore.isUsingEncKey("token1"), is(false));
      assertThat(restartStore.isUsingEncKey("token2"), is(false));
      assertThat(restartStore.isUsingEncKey("token3"), is(true));
      restartStore.shutdown();
    }

  }

  @Test
  public void testPartialWriteWithKeyAndThenRestartWithNewKey() throws Exception {
    String newKey = "";
    String oldTokenAndKey = "";
    File path = folder.newFolder();
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<>();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
          RestartStoreFactory.createStore(objectManager, path, properties);

      restartStore.startup().get();
      Map<String, String> map1 = createMap(restartStore, objectManager, 0);
      Map<String, String> map2 = createMap(restartStore, objectManager, 1);
      for (int i = 0; i < 20000; ++i) {
        map1.put(String.valueOf(i), "val" + i);
        map2.put(String.valueOf(i), "val" + i);
      }
      for(int i=0;i<1000;++i) {
        map1.remove(String.valueOf(i));
        map2.remove(String.valueOf(i));
      }
      
      oldTokenAndKey = properties.getProperty(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY.shortName());
      newKey = CipherHelper.generateNewKey();
      restartStore.handleEncKeyChange("token2", newKey);
      Thread.sleep(100);
      oldTokenAndKey = oldTokenAndKey.concat(MULTIPLE_TOKEN_KEY_DELIMETER + "token2" + TOKEN_KEY_DELIMITER + newKey);
      restartStore.shutdown();
    }

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<>();
      String latestKey = CipherHelper.generateNewKey();
      properties.setProperty(FrsProperty.STORE_ENCRYPTION_OLD_TOKENS_AND_KEYS.shortName(), oldTokenAndKey);
      properties.setProperty(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY.shortName(), "token3" + TOKEN_KEY_DELIMITER + latestKey);
      
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
          RestartStoreFactory.createStore(objectManager, path, properties);

      CountDownLatch latch = new CountDownLatch(1);
      List<String> expiredTokens = new ArrayList<>();
      
      restartStore.registerEncCompletionListener(event ->  {
        expiredTokens.addAll(event.getExpiredTokens());
        latch.countDown();
      });
      
      Map<String, String> map1 = createMap(restartStore, objectManager, 0);
      Map<String, String> map2 = createMap(restartStore, objectManager, 1);
      restartStore.startup().get();
      latch.await();
      for (int i = 1000; i < 20000; ++i) {
        assertThat(map1.get(String.valueOf(i)), is("val" + i));
        assertThat(map2.get(String.valueOf(i)), is("val" + i));
      }
      assertThat(map1.size(), is(19000));
      assertThat(map2.size(), is(19000));
      assertThat(expiredTokens.size(), is(2));
      assertTrue(expiredTokens.contains("token1"));
      assertTrue(expiredTokens.contains("token2"));
      restartStore.shutdown();
    }
  }
  
  private static Map<String, String> createMap(RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore,
                                               RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                                               int identifier) {
    SimpleRestartableMap map = new SimpleRestartableMap(identifier, restartStore, false);
    objectManager.registerObject(map);
    return map;
  }


  private void removeOldTokenAndKey() {
    properties.remove(FrsProperty.STORE_ENCRYPTION_OLD_TOKENS_AND_KEYS.shortName());
  }
}