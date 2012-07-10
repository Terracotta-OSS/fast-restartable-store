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
    initOutputDirectory();

    Properties properties = new Properties();
    properties.setProperty(FrsProperty.COMPACTOR_POLICY.shortName(), "NoCompactionPolicy");

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

    outputStore.shutdown();
    inputStore.shutdown();
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
      System.out.println(
              "Usage: java com.terracottatech.frs.OfflineCompactor inputFolder outputFolder");
      System.exit(-1);
    }
    File inputFile = new File(args[0]);
    if (!inputFile.isDirectory()) {
      System.out.println("Input folder " + inputFile + " is not a directory.");
      System.exit(-1);
    }
    File outputFile = new File(args[1]);
    System.out.println("Starting compaction.");
    new OfflineCompactor(inputFile, outputFile).compact();
    System.out.println("Compaction completed successfully.");
  }
}
