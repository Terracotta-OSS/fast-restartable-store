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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

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
      Map<String, String> map = createMap(restartStore, objectManager, 0);
      for (int i = 0; i < 100; ++i) {
        map.put(String.valueOf(i), "val" + i);
      }
      newKey = CipherHelper.generateNewKey();

      Thread t = new Thread(() -> {
        for (int i = 100; i < 110; ++i) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          map.put(String.valueOf(i), "val" + i);
        }
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
      properties.setProperty(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN.shortName(), "token1");
      properties.setProperty(FrsProperty.STORE_ENCRYPTION_NEW_KEY.shortName(), newKey);
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
          RestartStoreFactory.createStore(objectManager, path, properties);

      Map<String, String> map = createMap(restartStore, objectManager, 0);
      restartStore.startup().get();

      for (int i = 0; i < 110; ++i) {
        assertThat(map.get(String.valueOf(i)), is("val" + i));
      }
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
