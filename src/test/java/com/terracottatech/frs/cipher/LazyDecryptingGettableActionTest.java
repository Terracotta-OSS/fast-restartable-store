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

import com.terracottatech.frs.object.ObjectManager;

import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class LazyDecryptingGettableActionTest {

  private static final long INVALIDATED_LSN = 142L;
  private static final String TOKEN = "test-token";

  private ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private CipherManager cipherManager;
  private ByteBuffer identifier;
  private ByteBuffer[] encryptedBuffers;

  // Pre-built encrypted payload bytes (iv=4 bytes, payload=5 bytes)
  private static final byte[] IV_BYTES = {1, 2, 3, 4};
  private static final byte[] PAYLOAD_BYTES = {10, 20, 30, 40, 50};
  private static final byte[] KEY_BYTES = "key-data".getBytes(StandardCharsets.UTF_8);
  private static final byte[] VALUE_BYTES = "value-data".getBytes(StandardCharsets.UTF_8);

  @Before
  public void setUp() {
    objectManager = mock(ObjectManager.class);
    cipherManager = mock(CipherManager.class);

    identifier = ByteBuffer.wrap("my-identifier".getBytes(StandardCharsets.UTF_8));

    // Build the buffers array that decrypt() will read from:
    // [ivLength (4 bytes), payloadLength (4 bytes), iv bytes, encrypted payload bytes]
    ByteBuffer buf = ByteBuffer.allocate(4 + 4 + IV_BYTES.length + PAYLOAD_BYTES.length);
    buf.putInt(IV_BYTES.length);
    buf.putInt(PAYLOAD_BYTES.length);
    buf.put(IV_BYTES);
    buf.put(PAYLOAD_BYTES);
    buf.flip();
    encryptedBuffers = new ByteBuffer[]{buf};

    when(cipherManager.decrypt(any(ByteBuffer.class), any(ByteBuffer.class), eq(TOKEN)))
        .thenAnswer(invocation -> createDecryptedPayloadBuffer());
  }

  private ByteBuffer createDecryptedPayloadBuffer() {
    // [keyLength (4 bytes), valueLength (4 bytes), key bytes, value bytes]
    ByteBuffer decryptedPayloadBuf = ByteBuffer.allocate(4 + 4 + KEY_BYTES.length + VALUE_BYTES.length);
    decryptedPayloadBuf.putInt(KEY_BYTES.length);
    decryptedPayloadBuf.putInt(VALUE_BYTES.length);
    decryptedPayloadBuf.put(KEY_BYTES);
    decryptedPayloadBuf.put(VALUE_BYTES);
    decryptedPayloadBuf.flip();
    return decryptedPayloadBuf;
  }

  private LazyDecryptingGettableAction createAction() {
    return new LazyDecryptingGettableAction(objectManager, cipherManager, INVALIDATED_LSN,
        identifier, TOKEN, encryptedBuffers);
  }

  @Test
  public void testGetIdentifierReturnsConstructorValue() {
    LazyDecryptingGettableAction action = createAction();
    assertSame(identifier, action.getIdentifier());
  }

  @Test
  public void testGetTokenReturnsConstructorValue() {
    LazyDecryptingGettableAction action = createAction();
    assertEquals(TOKEN, action.getToken());
  }

  @Test
  public void testGetInvalidatedLsnsReturnsSingletonWithConstructorLsn() {
    LazyDecryptingGettableAction action = createAction();
    Set<Long> lsns = action.getInvalidatedLsns();
    assertNotNull(lsns);
    assertEquals(1, lsns.size());
    assertTrue(lsns.contains(INVALIDATED_LSN));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetLsnThrows() {
    createAction().getLsn();
  }

  @Test
  public void testRecordIsNoOp() {
    // Should not throw and should not interact with any dependency
    LazyDecryptingGettableAction action = createAction();
    action.record(99L);
    verify(objectManager, never()).replayPut(any(), any(), any(), any(long.class));
    verify(cipherManager, never()).decrypt(any(), any(), any());
  }

  @Test
  public void testGetKeyTriggersDecryptionOnFirstCall() {
    ByteBuffer expectedKey = ByteBuffer.wrap(KEY_BYTES);

    LazyDecryptingGettableAction action = createAction();
    ByteBuffer result = action.getKey();

    assertEquals(expectedKey, result);
    verify(cipherManager, times(1)).decrypt(any(ByteBuffer.class), any(ByteBuffer.class), eq(TOKEN));
  }

  @Test
  public void testGetValueTriggersDecryptionOnFirstCall() {
    ByteBuffer expectedValue = ByteBuffer.wrap(VALUE_BYTES);

    LazyDecryptingGettableAction action = createAction();
    ByteBuffer result = action.getValue();

    assertEquals(expectedValue, result);
    verify(cipherManager, times(1)).decrypt(any(ByteBuffer.class), any(ByteBuffer.class), eq(TOKEN));
  }

  @Test
  public void testGetKeyDoesNotDecryptTwiceOnSubsequentCalls() {
    LazyDecryptingGettableAction action = createAction();
    action.getKey();
    action.getKey();

    // decrypt() must only be invoked once
    verify(cipherManager, times(1)).decrypt(any(ByteBuffer.class), any(ByteBuffer.class), eq(TOKEN));
  }

  @Test
  public void testGetValueDoesNotDecryptTwiceOnSubsequentCalls() {
    LazyDecryptingGettableAction action = createAction();
    action.getValue();
    action.getValue();

    verify(cipherManager, times(1)).decrypt(any(ByteBuffer.class), any(ByteBuffer.class), eq(TOKEN));
  }

  @Test
  public void testGetKeyAndGetValueShareSingleDecryption() {
    LazyDecryptingGettableAction action = createAction();
    action.getKey();    // triggers decrypt
    action.getValue();  // reuses cached action

    // Still only one decrypt call
    verify(cipherManager, times(1)).decrypt(any(ByteBuffer.class), any(ByteBuffer.class), eq(TOKEN));
  }

  @Test
  public void testDecryptReceivesCorrectIvAndPayload() {
    createAction().getKey(); // trigger decrypt

    verify(cipherManager).decrypt(
        argThatHasBytes(PAYLOAD_BYTES),
        argThatHasBytes(IV_BYTES),
        eq(TOKEN));
  }

  @Test
  public void testReplayDecryptsAndCallsObjectManagerReplayPut() {
    ByteBuffer expectedKey = ByteBuffer.wrap(KEY_BYTES);
    ByteBuffer expectedValue = ByteBuffer.wrap(VALUE_BYTES);

    long replayLsn = 177L;
    LazyDecryptingGettableAction action = createAction();
    action.replay(replayLsn);

    verify(cipherManager, times(1)).decrypt(any(ByteBuffer.class), any(ByteBuffer.class), eq(TOKEN));
    verify(objectManager).replayPut(eq(identifier), eq(expectedKey), eq(expectedValue), eq(replayLsn));
  }
  @Test
  public void testSetDisposableAndClose() throws IOException {
    Closeable disposable = mock(Closeable.class);
    LazyDecryptingGettableAction action = createAction();
    action.setDisposable(disposable);
    action.close();
    verify(disposable).close();
  }

  @Test
  public void testCloseTwiceOnlyClosesDisposableOnce() throws IOException {
    Closeable disposable = mock(Closeable.class);
    LazyDecryptingGettableAction action = createAction();
    action.setDisposable(disposable);
    action.close();
    action.close(); // second call – disposable is already null
    verify(disposable, times(1)).close();
  }

  @Test
  public void testCloseWithNoDisposableIsNoOp() throws IOException {
    // Should not throw
    createAction().close();
  }

  @Test
  public void testDisposeClosesDisposable() throws IOException {
    Closeable disposable = mock(Closeable.class);
    LazyDecryptingGettableAction action = createAction();
    action.setDisposable(disposable);
    action.dispose();
    verify(disposable).close();
  }

  @Test
  public void testDisposeWrapsIOExceptionInRuntimeException() throws IOException {
    Closeable disposable = mock(Closeable.class);
    IOException cause = new IOException("disk error");
    org.mockito.Mockito.doThrow(cause).when(disposable).close();

    LazyDecryptingGettableAction action = createAction();
    action.setDisposable(disposable);

    try {
      action.dispose();
      fail("Expected RuntimeException");
    } catch (RuntimeException re) {
      assertSame(cause, re.getCause());
    }
  }

  @Test
  public void testImplementsEncryptedAction() {
    assertTrue(createAction() instanceof EncryptedAction);
  }

  /**
   * Utility: creates a ByteBuffer whose remaining bytes match {@code expected}.
   */
  private static ByteBuffer argThatHasBytes(byte[] expected) {
    return org.mockito.ArgumentMatchers.argThat(buf -> {
      if (buf == null || buf.remaining() != expected.length) return false;
      ByteBuffer dup = buf.duplicate();
      for (byte b : expected) {
        if (dup.get() != b) return false;
      }
      return true;
    });
  }
}
