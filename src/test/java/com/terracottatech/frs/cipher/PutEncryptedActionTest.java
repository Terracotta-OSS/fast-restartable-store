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
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.terracottatech.frs.PutAction;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.object.ObjectManager;

public class PutEncryptedActionTest {

  private PutAction mockAction;
  private CipherManager mockCipherManager;
  private ActionCodec mockCodec;
  private PutEncryptedAction encryptedAction;
  private ByteBuffer mockIV;
  private ByteBuffer token;

  @Before
  public void setUp() {
    mockAction = mock(PutAction.class);
    mockCipherManager = mock(CipherManager.class);
    mockCodec = mock(ActionCodec.class);
    mockIV = ByteBuffer.allocate(16);

    // Fill IV with some test data
    for (int i = 0; i < 16; i++) {
      mockIV.put((byte) i);
    }
    mockIV.flip();
    token = ByteBuffer.wrap("token".getBytes(StandardCharsets.UTF_8));
    when(mockCipherManager.generateInitializationVector()).thenReturn(mockIV);
    when(mockCipherManager.getCurrentToken()).thenReturn("token");
    encryptedAction = new PutEncryptedAction(mockAction, mockCipherManager);
  }

  @Test
  public void testConstructorAndDelegation() {
    // Test that the constructor properly initializes the object
    assertNotNull(encryptedAction);

    // Test delegation of record method
    encryptedAction.record(123L);
    verify(mockAction).record(123L);

    // Test delegation of replay method
    encryptedAction.replay(456L);
    verify(mockAction).replay(456L);

    // Test delegation of replayConcurrency method
    when(mockAction.replayConcurrency()).thenReturn(42);
    assertEquals(42, encryptedAction.replayConcurrency());
    verify(mockAction).replayConcurrency();
  }

  @Test
  public void testGetPayload() {
    // Setup mock behavior
    ByteBuffer[] originalPayload = new ByteBuffer[]{
        ByteBuffer.wrap("test payload".getBytes())
    };
    ByteBuffer[] encryptedPayload = new ByteBuffer[]{
        ByteBuffer.wrap("encrypted data".getBytes())
    };

    String id = "MyIdentifier";
    ByteBuffer identifer = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
    Set<Long> inv = new HashSet<>();
    inv.add(102L);
    when(mockAction.getIdentifier()).thenReturn(identifer);
    when(mockAction.getInvalidatedLsns()).thenReturn(inv);
    when(mockCodec.encode(mockAction)).thenReturn(originalPayload);
    when(mockCipherManager.encrypt(originalPayload, mockIV)).thenReturn(encryptedPayload);

    // Call the method under test
    ByteBuffer[] result = encryptedAction.getPayload(mockCodec);

    // Verify the result
    assertNotNull(result);
    assertEquals(5, result.length); // MetaData + Header + IV + token + encrypted payload

    // Verify the metadata
    ByteBuffer metaData = result[0];
    assertEquals(metaData.getLong(), 102L);
    assertEquals(metaData.getInt(), id.length());
    String decodedIdentifier = StandardCharsets.UTF_8.decode(metaData).toString();
    assertEquals(decodedIdentifier, id);

    // Verify the header contains the correct IV length, token length and payload length
    ByteBuffer header = result[1];
    header.rewind();
    assertEquals(mockIV.remaining(), header.getInt());
    assertEquals(token.remaining(), header.getInt());
    assertEquals("encrypted data".getBytes().length, header.getInt());

    // Verify the IV is included
    assertEquals(mockIV, result[2]);

    // Verify token is included
    assertEquals(token, result[3]);

    // Verify the encrypted payload is included
    assertEquals(encryptedPayload[0], result[4]);

    // Verify the interactions
    verify(mockCodec).encode(mockAction);
    verify(mockCipherManager).encrypt(originalPayload, mockIV);
  }

  @Test
  public void testEncryptedActionFactory() {
    // Setup
    PutEncryptedAction.EncryptedActionFactory factory =
        new PutEncryptedAction.EncryptedActionFactory(mockCipherManager);

    ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = mock(ObjectManager.class);

    // Prepare test data
    long expectedInvalidatedLsn = 12345L;
    String identifierString = "testIdentifier";
    byte[] identifierBytes = identifierString.getBytes(StandardCharsets.UTF_8);
    int identifierLength = identifierBytes.length;

    // Create buffers array with the expected format:
    // [invalidatedLsn (8 bytes), length (4 bytes), identifier bytes, ...]
    ByteBuffer buffer = ByteBuffer.allocate(8 + 4 + identifierLength + 100); // Extra space for other data
    buffer.putLong(expectedInvalidatedLsn);
    buffer.putInt(identifierLength);
    buffer.put(identifierBytes);
    buffer.flip();

    ByteBuffer[] buffers = new ByteBuffer[]{buffer};

    // Call the method under test
    Action result = factory.create(objectManager, mockCodec, buffers);

    // Verify the result
    assertNotNull(result);
    assertTrue(result instanceof LazyDecryptingPutAction);

    // Verify the created action has the correct identifier
    LazyDecryptingPutAction lazyAction = (LazyDecryptingPutAction) result;
    ByteBuffer resultIdentifier = lazyAction.getIdentifier();
    assertNotNull(resultIdentifier);

    byte[] resultIdentifierBytes = new byte[resultIdentifier.remaining()];
    resultIdentifier.get(resultIdentifierBytes);
    String resultIdentifierString = new String(resultIdentifierBytes, StandardCharsets.UTF_8);
    assertEquals(identifierString, resultIdentifierString);

    // Verify the invalidated LSNs
    Set<Long> invalidatedLsns = lazyAction.getInvalidatedLsns();
    assertNotNull(invalidatedLsns);
    assertEquals(1, invalidatedLsns.size());
    assertTrue(invalidatedLsns.contains(expectedInvalidatedLsn));
  }

  @Test
  public void testEqualsAndHashCode() {
    // Create two EncryptedAction objects with the same delegate
    PutEncryptedAction action1 = new PutEncryptedAction(mockAction, mockCipherManager);
    PutEncryptedAction action2 = new PutEncryptedAction(mockAction, mockCipherManager);

    // Test equals
    assertEquals(action1, action2);

    // Test hashCode
    assertEquals(action1.hashCode(), action2.hashCode());

    // Create an EncryptedAction with a different delegate
    PutAction differentMockAction = mock(PutAction.class);
    PutEncryptedAction differentAction = new PutEncryptedAction(differentMockAction, mockCipherManager);

    // They should not be equal
    assertTrue(!action1.equals(differentAction));
  }
}

// Made with Bob
