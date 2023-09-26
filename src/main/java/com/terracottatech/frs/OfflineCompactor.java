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

import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.object.NullObjectManager;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

/**
 * @author tim
 */
public class OfflineCompactor {
  private final File in;
  private final File out;

  public OfflineCompactor(File in, File out){
    this.in = in;
    this.out = out;

  }

  public void compact() throws Exception {
    if (!in.isDirectory()) {
      throw new IOException("Input folder " + in + " is not a directory.");
    }

    initOutputDirectory();

    Properties properties = new Properties();
    properties.setProperty(FrsProperty.COMPACTOR_POLICY.shortName(), "NoCompactionPolicy");
    properties.setProperty(FrsProperty.IO_NIO_ACCESS_METHOD.shortName(), "MAPPED");
    properties.setProperty(FrsProperty.IO_NIO_POOL_MEMORY_SIZE.shortName(), Long.toString(512 * 1024 * 1024));

    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> outputStore =
            RestartStoreFactory.createStore(
                    new NullObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>(), out,
                    properties);
    outputStore.startup().get();

    ForwardingObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager =
            new ForwardingObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>(outputStore);
    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> inputStore =
            RestartStoreFactory.createStore(objectManager, in, properties);
    inputStore.startup().get();

    inputStore.shutdown();
    outputStore.shutdown();
  }

  private void initOutputDirectory() throws IOException {
    if (out.exists()) {
      throw new IOException("Output directory " + out + " already exists.");
    }
    if (!out.mkdirs()) {
      throw new IOException("Failed to created output directory " + out);
    }

    File userPropertiesFile = new File(in, Configuration.USER_PROPERTIES_FILE);
    if (userPropertiesFile.exists()) {
      Properties userProperties = new Properties();
      FileInputStream fis = new FileInputStream(userPropertiesFile);
      try {
        userProperties.load(fis);
      } finally {
        fis.close();
      }

      File compactedUserPropertiesFile =
              new File(out, Configuration.USER_PROPERTIES_FILE);
      FileOutputStream fos = new FileOutputStream(compactedUserPropertiesFile);
      try {
        userProperties.store(fos, null);
      } finally {
        fos.close();
      }
    }
  }

  private static class ForwardingObjectManager<I, K, V> extends NullObjectManager<I, K ,V> {
    private final Transaction<I, K, V> transaction;

    private ForwardingObjectManager(RestartStore<I, K, V> backingStore) {
      this.transaction = backingStore.beginAutoCommitTransaction(false);
    }

    @Override
    public long getLsn(I id, K key) {
      return -1L;
    }

    @Override
    public void replayPut(I id, K key, V value, long lsn) {
      try {
        transaction.put(id, key, value);
      } catch (TransactionException e) {
        throw new RuntimeException("Failed to play back put.", e);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println(
              "Usage: java com.terracottatech.frs.OfflineCompactor inputFolder outputFolder");
      System.exit(1);
    }
    File inputFile = new File(args[0]);
    File outputFile = new File(args[1]);
    System.out.println("Starting compaction.");
    new OfflineCompactor(inputFile, outputFile).compact();
    System.out.println("Compaction completed successfully.");
  }
}
