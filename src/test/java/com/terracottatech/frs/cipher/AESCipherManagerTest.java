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
package com.terracottatech.frs.cipher;

import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.config.FrsProperty;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AESCipherManagerTest {

  private Configuration mockConfig;
  private List<byte[]> keys;
  private AESCipherManager cipherManager;
  private static final String TEST_ALGORITHM = "AES/CFB/PKCS5Padding";

  @Before
  public void setUp() throws Exception {
    // Create a mock configuration
    mockConfig = mock(Configuration.class);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_ALGORITHM)).thenReturn(TEST_ALGORITHM);

    // Generate a test key
    KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
    keyGenerator.init(256);
    SecretKey secretKey = keyGenerator.generateKey();
    byte[] keyBytes = secretKey.getEncoded();

    keys = Collections.singletonList(keyBytes);

    // Create the cipher manager
    cipherManager = new AESCipherManager(mockConfig, keys);
  }

  @Test
  public void testGenerateInitializationVector() {
    ByteBuffer iv = cipherManager.generateInitializationVector();
    assertNotNull("Initialization vector should not be null", iv);
    assertTrue("Initialization vector should have data", iv.remaining() > 0);
  }

  @Test
  public void testEncryptDecryptSimpleString() {
    // Create test data
    String testString = "This is a test string for encryption and decryption";
    ByteBuffer plainBuffer = ByteBuffer.wrap(testString.getBytes(StandardCharsets.UTF_8));
    ByteBuffer[] plainBuffers = new ByteBuffer[] { plainBuffer };

    // Generate IV
    ByteBuffer iv = cipherManager.generateInitializationVector();
    ByteBuffer ivCopy = ByteBuffer.allocate(iv.capacity());
    ivCopy.put(iv.duplicate());
    ivCopy.flip();

    // Encrypt
    ByteBuffer[] encryptedBuffers = cipherManager.encrypt(plainBuffers, iv);
    assertNotNull("Encrypted buffers should not be null", encryptedBuffers);
    assertTrue("Should have at least one encrypted buffer", encryptedBuffers.length > 0);

    // Combine encrypted buffers
    int totalSize = 0;
    for (ByteBuffer buffer : encryptedBuffers) {
      totalSize += buffer.remaining();
    }

    ByteBuffer combinedEncrypted = ByteBuffer.allocate(totalSize);
    for (ByteBuffer buffer : encryptedBuffers) {
      combinedEncrypted.put(buffer);
    }
    combinedEncrypted.flip();

    // Decrypt
    ByteBuffer decryptedBuffer = cipherManager.decrypt(combinedEncrypted, ivCopy);
    assertNotNull("Decrypted buffer should not be null", decryptedBuffer);

    // Verify decrypted content
    byte[] decryptedBytes = new byte[decryptedBuffer.remaining()];
    decryptedBuffer.get(decryptedBytes);
    String decryptedString = new String(decryptedBytes, StandardCharsets.UTF_8);

    assertEquals("Decrypted string should match original", testString, decryptedString);
  }

  @Test
  public void testEncryptDecryptLargeData() {
    // Create a larger test data (100KB)
    int dataSize = 100 * 1024; // 100KB
    byte[] testData = new byte[dataSize];
    for (int i = 0; i < dataSize; i++) {
      testData[i] = (byte) (i % 256);
    }

    ByteBuffer plainBuffer = ByteBuffer.wrap(testData);
    ByteBuffer[] plainBuffers = new ByteBuffer[] { plainBuffer };

    // Generate IV
    ByteBuffer iv = cipherManager.generateInitializationVector();
    ByteBuffer ivCopy = ByteBuffer.allocate(iv.capacity());
    ivCopy.put(iv.duplicate());
    ivCopy.flip();

    // Encrypt
    ByteBuffer[] encryptedBuffers = cipherManager.encrypt(plainBuffers, iv);

    // Combine encrypted buffers
    int totalSize = 0;
    for (ByteBuffer buffer : encryptedBuffers) {
      totalSize += buffer.remaining();
    }

    ByteBuffer combinedEncrypted = ByteBuffer.allocate(totalSize);
    for (ByteBuffer buffer : encryptedBuffers) {
      combinedEncrypted.put(buffer);
    }
    combinedEncrypted.flip();

    // Decrypt
    ByteBuffer decryptedBuffer = cipherManager.decrypt(combinedEncrypted, ivCopy);

    // Verify decrypted content
    byte[] decryptedBytes = new byte[decryptedBuffer.remaining()];
    decryptedBuffer.get(decryptedBytes);

    assertArrayEquals("Decrypted data should match original", testData, decryptedBytes);
  }

  @Test
  public void testEncryptDecryptMultipleBuffers() {
    // Create multiple test buffers
    List<ByteBuffer> buffers = new ArrayList<>();
    List<String> testStrings = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      String testString = "Test string " + i + " for encryption and decryption";
      testStrings.add(testString);
      buffers.add(ByteBuffer.wrap(testString.getBytes(StandardCharsets.UTF_8)));
    }

    ByteBuffer[] plainBuffers = buffers.toArray(new ByteBuffer[0]);

    // Generate IV
    ByteBuffer iv = cipherManager.generateInitializationVector();
    ByteBuffer ivCopy = ByteBuffer.allocate(iv.capacity());
    ivCopy.put(iv.duplicate());
    ivCopy.flip();

    // Encrypt
    ByteBuffer[] encryptedBuffers = cipherManager.encrypt(plainBuffers, iv);

    // Combine encrypted buffers
    int totalSize = 0;
    for (ByteBuffer buffer : encryptedBuffers) {
      totalSize += buffer.remaining();
    }

    ByteBuffer combinedEncrypted = ByteBuffer.allocate(totalSize);
    for (ByteBuffer buffer : encryptedBuffers) {
      combinedEncrypted.put(buffer);
    }
    combinedEncrypted.flip();

    // Decrypt
    ByteBuffer decryptedBuffer = cipherManager.decrypt(combinedEncrypted, ivCopy);

    // Verify decrypted content
    byte[] decryptedBytes = new byte[decryptedBuffer.remaining()];
    decryptedBuffer.get(decryptedBytes);
    String decryptedString = new String(decryptedBytes, StandardCharsets.UTF_8);

    // Combine original strings for comparison
    StringBuilder expectedBuilder = new StringBuilder();
    for (String str : testStrings) {
      expectedBuilder.append(str);
    }
    String expectedString = expectedBuilder.toString();

    assertEquals("Decrypted string should match original combined strings", expectedString, decryptedString);
  }

  @Test
  public void testValidateAlgorithm() {
    assertTrue("AES/CFB/PKCS5Padding should be a valid algorithm",
               CipherManager.validateAlgorithm("AES/CFB/PKCS5Padding"));
    assertTrue("AES/CBC/PKCS5Padding should be a valid algorithm",
               CipherManager.validateAlgorithm("AES/CBC/PKCS5Padding"));
    assertTrue("AES/GCM/NoPadding should be a valid algorithm",
               CipherManager.validateAlgorithm("AES/GCM/NoPadding"));
  }
}

// Made with Bob
