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

import com.terracottatech.frs.GettableAction;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.object.ObjectManager;

public class EncryptedGettableActionTest {

  private GettableAction mockAction;
  private CipherManager mockCipherManager;
  private ActionCodec mockCodec;
  private EncryptedGettableAction encryptedAction;
  private ByteBuffer mockIV;
  private ByteBuffer token;

  @Before
  public void setUp() {
    mockAction = mock(GettableAction.class);
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
    encryptedAction = new EncryptedGettableAction(mockAction, mockCipherManager);
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
    String token = "token1";
    when(mockAction.getIdentifier()).thenReturn(identifer);
    when(mockAction.getInvalidatedLsns()).thenReturn(inv);
    when(mockCipherManager.getCurrentToken()).thenReturn(token);
    when(mockCodec.encode(mockAction)).thenReturn(originalPayload);
    when(mockCipherManager.encrypt(originalPayload, mockIV)).thenReturn(encryptedPayload);

    // Call the method under test
    ByteBuffer[] result = encryptedAction.getPayload(mockCodec);

    // Verify the result
    assertNotNull(result);
    assertEquals(4, result.length); // MetaData + Header + IV + encrypted payload

    // Verify the metadata
    ByteBuffer metaData = result[0];
    assertEquals(metaData.getLong(), 102L);
    assertEquals(metaData.getInt(), id.length());
    byte[] idbytes = new byte[id.length()];
    metaData.get(idbytes);
    String decodedIdentifier = new String(idbytes, StandardCharsets.UTF_8);
    assertEquals(decodedIdentifier, id);
    assertEquals(metaData.getInt(), token.length());
    String decodedToken = StandardCharsets.UTF_8.decode(metaData).toString();
    assertEquals(decodedToken, token);

    // Verify the header contains the correct IV length and payload length
    ByteBuffer header = result[1];
    header.rewind();
    assertEquals(mockIV.remaining(), header.getInt());
    assertEquals("encrypted data".getBytes().length, header.getInt());

    // Verify the IV is included
    assertEquals(mockIV, result[2]);

    // Verify the encrypted payload is included
    assertEquals(encryptedPayload[0], result[3]);

    // Verify the interactions
    verify(mockCodec).encode(mockAction);
    verify(mockCipherManager).encrypt(originalPayload, mockIV);
  }

  @Test
  public void testEncryptedActionFactory() {
    // Setup
    EncryptedGettableAction.EncryptedActionFactory factory =
        new EncryptedGettableAction.EncryptedActionFactory(mockCipherManager);

    ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = mock(ObjectManager.class);

    // Prepare test data
    long expectedInvalidatedLsn = 12345L;
    String identifierString = "testIdentifier";
    byte[] identifierBytes = identifierString.getBytes(StandardCharsets.UTF_8);
    int identifierLength = identifierBytes.length;
    String token = "mytoken";
    byte[] tokenBytes = token.getBytes(StandardCharsets.UTF_8);
    int tokenLength = tokenBytes.length;
    // Create buffers array with the expected format:
    // [invalidatedLsn (8 bytes), length (4 bytes), identifier bytes, tokenlength (4bytes), token bytes, ...]
    ByteBuffer buffer = ByteBuffer.allocate(8 + 4 + identifierLength + 4 + tokenLength + 100); // Extra space for other data
    buffer.putLong(expectedInvalidatedLsn);
    buffer.putInt(identifierLength);
    buffer.put(identifierBytes);
    buffer.putInt(tokenLength);
    buffer.put(tokenBytes);
    buffer.flip();

    ByteBuffer[] buffers = new ByteBuffer[]{buffer};

    // Call the method under test
    Action result = factory.create(objectManager, mockCodec, buffers);

    // Verify the result
    assertNotNull(result);
    assertTrue(result instanceof LazyDecryptingGettableAction);

    // Verify the created action has the correct identifier
    LazyDecryptingGettableAction lazyAction = (LazyDecryptingGettableAction) result;
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

    // Verify the token
    String resultToken = lazyAction.getToken();
    assertEquals(token, resultToken);
  }

  @Test
  public void testEqualsAndHashCode() {
    // Create two EncryptedAction objects with the same delegate
    EncryptedGettableAction action1 = new EncryptedGettableAction(mockAction, mockCipherManager);
    EncryptedGettableAction action2 = new EncryptedGettableAction(mockAction, mockCipherManager);

    // Test equals
    assertEquals(action1, action2);

    // Test hashCode
    assertEquals(action1.hashCode(), action2.hashCode());

    // Create an EncryptedAction with a different delegate
    GettableAction differentMockAction = mock(GettableAction.class);
    EncryptedGettableAction differentAction = new EncryptedGettableAction(differentMockAction, mockCipherManager);

    // They should not be equal
    assertTrue(!action1.equals(differentAction));
  }
}

// Made with Bob
