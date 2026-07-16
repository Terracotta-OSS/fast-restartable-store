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
import java.util.Map;
import java.util.Properties;

import static com.terracottatech.frs.cipher.EncryptionCodecImpl.TOKEN_KEY_DELIMITER;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;

public class RestartStoreEncryptionEnabledTest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private Properties properties = new Properties();

  @Before
  public void setUp() {
    properties = CipherHelper.configure(false, properties);
  }

  @Test
  public void testStoreFromNoEncryptionToEncryption() throws Exception {
    File path = folder.newFolder();
    String newKey = "";
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<>();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
          RestartStoreFactory.createStore(objectManager, path, properties);

      restartStore.startup().get();
      Map<String, String> map1 = createMap(restartStore, objectManager, 0);
      Map<String, String> map2 = createMap(restartStore, objectManager, 1);
      for (int i = 0; i < 100; ++i) {
        map1.put(String.valueOf(i), "val" + i);
        map2.put(String.valueOf(i), "val" + i);
      }
      newKey = CipherHelper.generateNewKey();

      Thread t = new Thread(() -> {
        for (int i = 100, j = 0; i < 110; ++i, ++j) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          assertThat(map1.get(String.valueOf(j)), is("val" + j));
          map1.remove(String.valueOf(j));
          map1.put(String.valueOf(i), "val" + i);
        }
        map2.clear();
      });
      t.start();

      restartStore.handleEncKeyChange("token1", newKey);

      Thread.sleep(3000);

      t.join();

      assertThat(restartStore.isUsingEncKey("token1"), is(true));
      restartStore.shutdown();
    }

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<>();
      properties.setProperty(FrsProperty.STORE_ENCRYPTION_ENABLE.shortName(), "true");
      properties.setProperty(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY.shortName(), "token1" + TOKEN_KEY_DELIMITER + newKey);
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
          RestartStoreFactory.createStore(objectManager, path, properties);

      Map<String, String> map1 = createMap(restartStore, objectManager, 0);
      Map<String, String> map2 = createMap(restartStore, objectManager, 1);
      restartStore.startup().get();

      for (int i = 0; i < 10; ++i) {
        assertNull(map1.get(String.valueOf(i)));
      }
      for (int i = 10; i < 110; ++i) {
        assertThat(map1.get(String.valueOf(i)), is("val" + i));
      }
      assertThat(map2.size(), is(0));
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
}
