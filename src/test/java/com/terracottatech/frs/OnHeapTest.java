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
package com.terracottatech.frs;

import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.RegisterableObjectManager;
import com.terracottatech.frs.object.SimpleRestartableMap;
import com.terracottatech.frs.util.JUnitTestFolder;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 *
 * @author mscott
 */
public abstract class OnHeapTest {
    DecimalFormat df = new DecimalFormat("0000000000");
        
    @Rule
    public JUnitTestFolder folder = new JUnitTestFolder();

    public abstract Properties configure(Properties props);
    
    private Map<String, String> createMap(int id, RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager, RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore) {
      SimpleRestartableMap map = new SimpleRestartableMap(id, restartStore, false);
      objectManager.registerObject(map);
      return map;
    }

    private RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> createObjectManager() {
      return new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>();
    }

    private RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> createStore(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager) throws
            RestartStoreException, IOException {
      return RestartStoreFactory.createStore(objectManager, folder.getRoot(),
                                             configure(new Properties()));
    }
    
    private int addTransaction(int count, Map<String, String> map) throws Exception {
        String[] r = {"foo","bar","baz","boo","tim","sar","myr","chr"};
        String sk = df.format(count);
        String sv = r[(int)(Math.random()*r.length)%r.length];
        map.put(sk, sv);
        return stringSize(sk) + stringSize(sv);
    }

    private int stringSize(String s) {
      return s.getBytes().length;
    }

    @Test
    public void testIt() throws Exception {
      int count = 0;
      {
        RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = createObjectManager();
        RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
                createStore(objectManager);
        Map<String, String> map = createMap(0, objectManager, restartStore);
        restartStore.startup().get();

        long bin = 0;
        long time = System.nanoTime();
        while ( bin < 1 * 1024 * 1024 ) {
            bin += addTransaction(count++, map);
        }
        System.out.format("%.6f sec.\n",(System.nanoTime() - time)/(1e9));
        restartStore.shutdown();
        System.out.format("bytes in: %d\n",bin);
      }

        
      File[] list = folder.getRoot().listFiles();
      long fl = 0;
      for ( File f : list  ){
          System.out.println(f.getName() + " " + f.length());
          fl += f.length();
      }
      System.out.println("file total: " + fl);

      {
        RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = createObjectManager();
        RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
                createStore(objectManager);
        Map<String, String> map = createMap(0, objectManager, restartStore);
        long time = System.nanoTime();
        restartStore.startup().get();
        System.out.format("%.6f sec.\n",(System.nanoTime() - time)/(1e9));
        System.out.println("Recovered " + objectManager.sizeInBytes() + " bytes.");

        System.out.println("=========");

        assertThat(map.size(), is(count));

        for (int x=0;x<100;x++) addTransaction(count + x, map);

        restartStore.shutdown();
      }

      {
        RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = createObjectManager();
        RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
                createStore(objectManager);
        Map<String, String> map = createMap(0, objectManager, restartStore);
        long time = System.nanoTime();
        restartStore.startup().get();
        System.out.format("%.6f sec.\n",(System.nanoTime() - time)/(1e9));
        System.out.println("Recovered " + objectManager.sizeInBytes() + " bytes.");

        assertThat(map.size(), is(count + 100));

        restartStore.shutdown();
      }
    }


}
