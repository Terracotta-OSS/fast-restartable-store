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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AESCipherManagerTest {

  private static final String TEST_ALGORITHM = "AES/CFB/PKCS5Padding";
  private AESCipherManager cipherManager;

  @Before
  public void setUp() throws Exception {
    // Create a mock configuration
    // Generate a test key
    KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
    keyGenerator.init(256);
    SecretKey secretKey = keyGenerator.generateKey();
    byte[] keyBytes = secretKey.getEncoded();

    List<byte[]> keys = Collections.singletonList(keyBytes);

    // Create the cipher manager
    cipherManager = new AESCipherManager(TEST_ALGORITHM, keys);
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

    // Generate IV
    ByteBuffer iv = cipherManager.generateInitializationVector();
    ByteBuffer ivCopy = ByteBuffer.allocate(iv.capacity());
    ivCopy.put(iv.duplicate());
    ivCopy.flip();

    // Encrypt
    ByteBuffer encryptedBuffer = cipherManager.encrypt(plainBuffer, iv);
    assertNotNull("Encrypted buffers should not be null", encryptedBuffer);

    // Decrypt
    ByteBuffer decryptedBuffer = cipherManager.decrypt(encryptedBuffer, ivCopy);
    assertNotNull("Decrypted buffer should not be null", decryptedBuffer);

    // Verify decrypted content
    byte[] decryptedBytes = new byte[decryptedBuffer.remaining()];
    decryptedBuffer.get(decryptedBytes);
    String decryptedString = new String(decryptedBytes, StandardCharsets.UTF_8);

    assertEquals("Decrypted string should match original", testString, decryptedString);
  }
}
