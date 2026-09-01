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

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link AESGCMCipherDelegate} in isolation.
 */
public class AESGCMCipherDelegateTest {

  private AESGCMCipherDelegate delegate;
  private SecretKey secretKey;

  @Before
  public void setUp() throws Exception {
    delegate = new AESGCMCipherDelegate();
    KeyGenerator kg = KeyGenerator.getInstance("AES");
    kg.init(256);
    secretKey = kg.generateKey();
  }

  @Test
  public void getCipher_returnsAesGcmInstance() {
    Cipher cipher = delegate.getCipher();
    assertNotNull(cipher);
    assertEquals(AESGCMCipherDelegate.ALGORITHM, cipher.getAlgorithm());
  }

  @Test
  public void getCipher_returnsFreshInstanceEachCall() {
    Cipher c1 = delegate.getCipher();
    Cipher c2 = delegate.getCipher();
    assertNotSame("Each call must return a distinct Cipher object", c1, c2);
  }

  @Test
  public void getCipherWithKey_worksWithNonArrayBackedIv() {
    // Force a non-array-backed ByteBuffer for the IV path that copies bytes manually
    ByteBuffer iv = delegate.generateInitializationVector();
    ByteBuffer directIv = ByteBuffer.allocateDirect(iv.remaining());
    directIv.put(iv);
    directIv.flip();

    Cipher cipher = delegate.getCipher(secretKey, Cipher.ENCRYPT_MODE, directIv);
    assertNotNull(cipher);
  }

  @Test
  public void generateIV_correctLengthForGcm() {
    ByteBuffer iv = delegate.generateInitializationVector();
    // GCM standard IV length is 12 bytes
    assertEquals(12, iv.remaining());
  }

  @Test
  public void generateIV_differentValuesEachCall() {
    ByteBuffer iv1 = delegate.generateInitializationVector();
    ByteBuffer iv2 = delegate.generateInitializationVector();
    // Two independently generated IVs must not be identical
    byte[] b1 = new byte[iv1.remaining()];
    byte[] b2 = new byte[iv2.remaining()];
    iv1.get(b1);
    iv2.get(b2);
    boolean allSame = true;
    for (int i = 0; i < b1.length; i++) {
      if (b1[i] != b2[i]) {
        allSame = false;
        break;
      }
    }
    // While theoretically two random IVs could collide, the probability is negligible
    // and this test would only fail once in 2^96 runs
    assertTrue("Two independently generated IVs should differ", !allSame || b1.length == 0);
  }

  @Test
  public void encryptDecrypt_roundTrip() {
    String plaintext = "Hello, AES/GCM/NoPadding!";
    ByteBuffer[] input = {ByteBuffer.wrap(plaintext.getBytes(StandardCharsets.UTF_8))};

    ByteBuffer iv = delegate.generateInitializationVector();
    ByteBuffer ivForDecrypt = copyBuffer(iv);

    ByteBuffer[] encrypted = delegate.encrypt(input, secretKey, iv);
    ByteBuffer combined = combine(encrypted);

    ByteBuffer decrypted = delegate.decrypt(combined, secretKey, ivForDecrypt);
    byte[] result = toBytes(decrypted);

    assertArrayEquals(plaintext.getBytes(StandardCharsets.UTF_8), result);
  }

  @Test
  public void encryptDecrypt_multipleInputBuffers() {
    // Three independent plaintext chunks supplied as separate ByteBuffers
    String chunk1 = "Hello, ";
    String chunk2 = "AES/GCM";
    String chunk3 = "/NoPadding!";
    ByteBuffer[] input = {
        ByteBuffer.wrap(chunk1.getBytes(StandardCharsets.UTF_8)),
        ByteBuffer.wrap(chunk2.getBytes(StandardCharsets.UTF_8)),
        ByteBuffer.wrap(chunk3.getBytes(StandardCharsets.UTF_8))
    };

    byte[] expectedPlaintext = (chunk1 + chunk2 + chunk3).getBytes(StandardCharsets.UTF_8);

    ByteBuffer iv = delegate.generateInitializationVector();
    ByteBuffer ivForDecrypt = copyBuffer(iv);

    ByteBuffer[] encrypted = delegate.encrypt(input, secretKey, iv);

    // Decrypt and verify the full plaintext is recovered
    ByteBuffer combined = combine(encrypted);
    ByteBuffer decrypted = delegate.decrypt(combined, secretKey, ivForDecrypt);
    assertArrayEquals(expectedPlaintext, toBytes(decrypted));
  }

  @Test
  public void encrypt_producesLargerOutput() {
    byte[] plain = "data".getBytes(StandardCharsets.UTF_8);
    ByteBuffer[] input = {ByteBuffer.wrap(plain)};

    ByteBuffer iv = delegate.generateInitializationVector();
    ByteBuffer[] encrypted = delegate.encrypt(input, secretKey, iv);

    int encryptedSize = 0;
    for (ByteBuffer b : encrypted) encryptedSize += b.remaining();

    // GCM appends a 16-byte auth tag so ciphertext > plaintext
    assertTrue("GCM ciphertext must be larger than plaintext due to auth tag",
        encryptedSize > plain.length);
  }

  private static ByteBuffer copyBuffer(ByteBuffer src) {
    ByteBuffer copy = ByteBuffer.allocate(src.remaining());
    copy.put(src.duplicate());
    copy.flip();
    return copy;
  }

  private static ByteBuffer combine(ByteBuffer[] buffers) {
    int total = 0;
    for (ByteBuffer b : buffers) total += b.remaining();
    ByteBuffer out = ByteBuffer.allocate(total);
    for (ByteBuffer b : buffers) out.put(b);
    out.flip();
    return out;
  }

  private static byte[] toBytes(ByteBuffer buf) {
    byte[] bytes = new byte[buf.remaining()];
    buf.get(bytes);
    return bytes;
  }
}
