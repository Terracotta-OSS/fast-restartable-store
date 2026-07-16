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

import com.terracottatech.frs.GettableAction;
import com.terracottatech.frs.MapActions;
import com.terracottatech.frs.PutAction;
import com.terracottatech.frs.RemoveAction;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionCodecImpl;
import com.terracottatech.frs.action.ActionSubCodec;
import com.terracottatech.frs.compaction.CompactionAction;
import com.terracottatech.frs.compaction.CompactionActions;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.config.FrsProperty;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.SimpleObjectManagerEntry;
import com.terracottatech.frs.transaction.TransactionActions;
import org.junit.Before;
import org.junit.Test;

import static com.terracottatech.frs.cipher.EncryptionCodecImpl.MULTIPLE_TOKEN_KEY_DELIMITER;
import static com.terracottatech.frs.cipher.EncryptionCodecImpl.TOKEN_KEY_DELIMITER;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EncryptionCodecImplTest {

  private static final int MAP_COLLECTION_ID = 0;
  private static final int TRANSACTION_COLLECTION_ID = 1;
  private static final int COMPACTION_COLLECTION_ID = 2;

  private Configuration mockConfig;
  private ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> mockActionCodec;
  private String testKey1;
  private String testKey2;
  private String testKey3;
  private static final String TOKEN1 = "token1";
  private static final String TOKEN2 = "token2";
  private static final String TOKEN3 = "token3";

  private ByteBuffer identifier;
  private ByteBuffer key;
  private ByteBuffer value;

  @Before
  public void setUp() throws Exception {
    mockConfig = mock(Configuration.class);
    objectManager = mock(ObjectManager.class);
    mockActionCodec = mock(ActionCodec.class);
    // Generate test keys
    KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
    keyGenerator.init(256);
    SecretKey secretKey1 = keyGenerator.generateKey();
    testKey1 = Base64.getEncoder().encodeToString(secretKey1.getEncoded());

    SecretKey secretKey2 = keyGenerator.generateKey();
    testKey2 = Base64.getEncoder().encodeToString(secretKey2.getEncoded());

    SecretKey secretKey3 = keyGenerator.generateKey();
    testKey3 = Base64.getEncoder().encodeToString(secretKey3.getEncoded());

    identifier = buf("identifier");
    key = buf("the-key");
    value = buf("the-value");
  }

  @Test
  public void testConstructorWithEncryptionDisabled() {
    // Setup config for disabled encryption
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(false);

    // Create encCodec
    EncryptionCodec encCodec = new EncryptionCodecImpl(mockConfig, mockActionCodec);

    // Verify it uses NoEncryptionHandler
    assertNotNull("encCodec should not be null", encCodec);
    assertTrue("Previous token should not be present when encryption is disabled",
        encCodec.getPreviousTokens().isEmpty());
  }

  @Test
  public void testConstructorWithEncryptionEnabledSingleKey() {
    // Setup config for enabled encryption with single key
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(true);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY)).thenReturn(TOKEN1 + TOKEN_KEY_DELIMITER + testKey1);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_OLD_TOKENS_AND_KEYS)).thenReturn(null);

    // Create encCodec
    EncryptionCodec encCodec = new EncryptionCodecImpl(mockConfig, mockActionCodec);

    // Verify
    assertNotNull("encCodec should not be null", encCodec);
    assertTrue("Should be using the new token", encCodec.isUsingEncKey(TOKEN1));
    assertTrue("Previous token should not be present with single key",
        encCodec.getPreviousTokens().isEmpty());
  }

  @Test
  public void testConstructorWithEncryptionEnabledTwoKeys() {
    // Setup config for enabled encryption with two keys
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(true);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY)).thenReturn(TOKEN2 + TOKEN_KEY_DELIMITER + testKey2);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_OLD_TOKENS_AND_KEYS)).thenReturn(TOKEN1 + TOKEN_KEY_DELIMITER + testKey1);

    // Create encCodec
    EncryptionCodec encCodec = new EncryptionCodecImpl(mockConfig, mockActionCodec);

    // Verify
    assertNotNull("encCodec should not be null", encCodec);
    assertTrue("Should be using the new token", encCodec.isUsingEncKey(TOKEN2));
    assertTrue("Should be using the old token", encCodec.isUsingEncKey(TOKEN1));
    assertFalse("Previous token should be present with two keys",
        encCodec.getPreviousTokens().isEmpty());
    assertEquals("Previous token should be TOKEN1", TOKEN1, encCodec.getPreviousTokens().iterator().next());
  }

  @Test
  public void testGetPreviousTokenWithEncryptionDisabled() {
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(false);

    EncryptionCodec encCodec = new EncryptionCodecImpl(mockConfig, mockActionCodec);

    assertTrue("Previous token should not be present when encryption is disabled",
        encCodec.getPreviousTokens().isEmpty());
  }

  @Test
  public void testIsUsingEncKeyWithEncryptionEnabled() {
    // Setup config for enabled encryption
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(true);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY)).thenReturn(TOKEN2 + TOKEN_KEY_DELIMITER + testKey2);

    EncryptionCodec encCodec = new EncryptionCodecImpl(mockConfig, mockActionCodec);

    assertTrue("Should return true for existing token", encCodec.isUsingEncKey(TOKEN2));
    assertFalse("Should return false for non-existing token", encCodec.isUsingEncKey("nonExistentToken"));
  }

  @Test
  public void testIsUsingEncKeyWithEncryptionDisabled() {
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(false);

    EncryptionCodec encCodec = new EncryptionCodecImpl(mockConfig, mockActionCodec);

    assertFalse("Should return false for any token when encryption is disabled",
        encCodec.isUsingEncKey(TOKEN1));
  }

  @Test
  public void testAddWithEncryptionEnabled() {
    // Setup config for enabled encryption with single key
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(true);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY)).thenReturn(TOKEN1 + TOKEN_KEY_DELIMITER + testKey1);

    EncryptionCodec encCodec = new EncryptionCodecImpl(mockConfig, mockActionCodec);
    ActionSubCodec<ByteBuffer, ByteBuffer, ByteBuffer, PutAction> mockSubCodec = mock(ActionSubCodec.class);
    encCodec.registerAndBind(PutAction.class, 0, 0, mockSubCodec);
    encCodec.registerAndBind(CompactionAction.class, 2, 0, mockSubCodec);

    // Verify that new encryption codec was registered
    verify(mockActionCodec).registerCodec(eq(99), eq(0), any(ActionSubCodec.class));
    verify(mockActionCodec).registerCodec(eq(101), eq(0), any(ActionSubCodec.class));

    // Verify bind called
    verify(mockActionCodec).bind(eq(PutAction.class), eq(99), eq(0));
    verify(mockActionCodec).bind(eq(CompactionAction.class), eq(101), eq(0));

    // Add a new token
    encCodec.add(TOKEN2, Base64.getDecoder().decode(testKey2));

    // Verify the new token was added
    assertTrue("Should be using the newly added token", encCodec.isUsingEncKey(TOKEN2));
    assertTrue("Should still be using the original token", encCodec.isUsingEncKey(TOKEN1));
  }

  @Test
  public void testAddWithEncryptionDisabled() {
    // Setup config for disabled encryption
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(false);

    EncryptionCodec encCodec = new EncryptionCodecImpl(mockConfig, mockActionCodec);

    ActionSubCodec<ByteBuffer, ByteBuffer, ByteBuffer, PutAction> mockSubCodec = mock(ActionSubCodec.class);
    encCodec.registerAndBind(PutAction.class, 0, 0, mockSubCodec);
    encCodec.registerAndBind(CompactionAction.class, 2, 0, mockSubCodec);
    // Add a token (should enable encryption)
    encCodec.add(TOKEN1, Base64.getDecoder().decode(testKey1));

    // Verify the token was added and encryption is now enabled
    assertTrue("Should be using the added token", encCodec.isUsingEncKey(TOKEN1));
    // Verify that new encryption codec was registered
    verify(mockActionCodec).registerCodec(eq(99), eq(0), any(ActionSubCodec.class));
    verify(mockActionCodec).registerCodec(eq(101), eq(0), any(ActionSubCodec.class));
  }

  @Test
  public void testRemoveWithEncryptionEnabled() {
    // Setup config for enabled encryption with two keys
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(true);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY)).thenReturn(TOKEN2 + TOKEN_KEY_DELIMITER + testKey2);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_OLD_TOKENS_AND_KEYS)).thenReturn(TOKEN1 + TOKEN_KEY_DELIMITER + testKey1);

    EncryptionCodec encCodec = new EncryptionCodecImpl(mockConfig, mockActionCodec);

    // Verify both tokens exist
    assertTrue("TOKEN1 should exist before removal", encCodec.isUsingEncKey(TOKEN1));
    assertTrue("TOKEN2 should exist before removal", encCodec.isUsingEncKey(TOKEN2));

    // Remove TOKEN1
    encCodec.remove(Collections.singletonList(TOKEN1));

    // Verify TOKEN1 was removed
    assertFalse("TOKEN1 should not exist after removal", encCodec.isUsingEncKey(TOKEN1));
    assertTrue("TOKEN2 should still exist after removing TOKEN1", encCodec.isUsingEncKey(TOKEN2));
  }

  @Test
  public void testMultipleAddOperations() {
    // Setup config for enabled encryption
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(true);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY)).thenReturn(TOKEN1 + TOKEN_KEY_DELIMITER + testKey1);

    EncryptionCodec encCodec = new EncryptionCodecImpl(mockConfig, mockActionCodec);

    // Add multiple tokens
    encCodec.add(TOKEN2, Base64.getDecoder().decode(testKey2));
    encCodec.add(TOKEN3, Base64.getDecoder().decode(testKey3));

    // Verify all tokens are present
    assertTrue("TOKEN1 should be present", encCodec.isUsingEncKey(TOKEN1));
    assertTrue("TOKEN2 should be present", encCodec.isUsingEncKey(TOKEN2));
    assertTrue("token3 should be present", encCodec.isUsingEncKey(TOKEN3));
  }

  @Test
  public void testMultipleRemoveOperations() {
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(true);
    String oldTokenAndKeys = TOKEN1 + TOKEN_KEY_DELIMITER + testKey1 + MULTIPLE_TOKEN_KEY_DELIMITER +
        TOKEN2 + TOKEN_KEY_DELIMITER + testKey2;
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_OLD_TOKENS_AND_KEYS)).thenReturn(oldTokenAndKeys);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY)).thenReturn(TOKEN3 + TOKEN_KEY_DELIMITER + testKey3);

    EncryptionCodec encCodec = new EncryptionCodecImpl(mockConfig, mockActionCodec);

    assertTrue("TOKEN1 should be present", encCodec.isUsingEncKey(TOKEN1));
    assertTrue("TOKEN2 should be present", encCodec.isUsingEncKey(TOKEN2));
    assertTrue("token3 should be present", encCodec.isUsingEncKey(TOKEN3));

    encCodec.remove(encCodec.getPreviousTokens());

    assertFalse(encCodec.isUsingEncKey(TOKEN1));
    assertFalse(encCodec.isUsingEncKey(TOKEN2));
    assertTrue(encCodec.isUsingEncKey(TOKEN3));
  }

  @Test
  public void testPutActionEncodeProducesEncryptedPayload() {
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(true);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY)).thenReturn(TOKEN1 + TOKEN_KEY_DELIMITER + testKey1);
    EncryptionCodec encryptionCodec = new EncryptionCodecImpl(mockConfig, new ActionCodecImpl(objectManager));
    register(encryptionCodec);
    PutAction put = putAction(-1L);

    ByteBuffer[] encoded = encryptionCodec.encode(put);

    assertNotNull(encoded);
    assertTrue(encoded.length > 0);

    // The raw concatenated bytes must NOT equal a plain unencrypted encoding
    ByteBuffer[] plainEncoded = plainCodec().encode(put);
    assertThat(flatten(encoded), not(is(flatten(plainEncoded))));
  }

  @Test
  public void testPutActionRoundTripRestoresKeyAndValue() {
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(true);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY)).thenReturn(TOKEN1 + TOKEN_KEY_DELIMITER + testKey1);
    EncryptionCodec encryptionCodec = new EncryptionCodecImpl(mockConfig, new ActionCodecImpl(objectManager));
    register(encryptionCodec);
    PutAction put = putAction(-1L);

    ByteBuffer[] encoded = encryptionCodec.encode(put);
    Action decoded = encryptionCodec.decode(encoded);

    assertThat(decoded, instanceOf(GettableAction.class));
    GettableAction got = (GettableAction) decoded;
    assertEquals(key, got.getKey());
    assertEquals(value, got.getValue());
  }

  @Test
  public void testCompactionActionEncodeProducesEncryptedPayload() {
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(true);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY)).thenReturn(TOKEN1 + TOKEN_KEY_DELIMITER + testKey1);
    EncryptionCodec encryptionCodec = new EncryptionCodecImpl(mockConfig, new ActionCodecImpl(objectManager));
    register(encryptionCodec);
    CompactionAction compactionAction = compactionAction(105L);

    ByteBuffer[] encoded = encryptionCodec.encode(compactionAction);

    assertNotNull(encoded);
    assertTrue(encoded.length > 0);

    // The raw concatenated bytes must NOT equal a plain unencrypted encoding
    ByteBuffer[] plainEncoded = plainCodec().encode(compactionAction);
    assertThat(flatten(encoded), not(is(flatten(plainEncoded))));
  }

  @Test
  public void testEncodeDecodeRoundTripForRemoveAction() {
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(true);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY)).thenReturn(TOKEN1 + TOKEN_KEY_DELIMITER + testKey1);
    EncryptionCodec encryptionCodec = new EncryptionCodecImpl(mockConfig, new ActionCodecImpl(objectManager));
    register(encryptionCodec);

    // RemoveAction is pass-through — encode via encryptionCodec must equal encode via raw codec
    // We just verify the bytes are identical to the plain codec output.
    ByteBuffer[] plainRemove = plainCodec().encode(removeAction());

    // Handler must produce identical bytes for a non-encrypted action
    ByteBuffer[] encCodecRemove = encryptionCodec.encode(removeAction());
    assertArrayEquals(flatten(plainRemove).array(), flatten(encCodecRemove).array());
  }

  @Test
  public void testPutActionEncodedWithToken1CanBeDecodedAfterKeyRotation() {
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(true);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY)).thenReturn(TOKEN1 + TOKEN_KEY_DELIMITER + testKey1);
    EncryptionCodec encryptionCodec = new EncryptionCodecImpl(mockConfig, new ActionCodecImpl(objectManager));
    register(encryptionCodec);
    ByteBuffer[] encoded = encryptionCodec.encode(putAction(-1L));

    // Rotate to TOKEN2 on the same handler — TOKEN1 is still present
    encryptionCodec.add(TOKEN2, Base64.getDecoder().decode(testKey2));

    // Decode: the encoded payload carries "token1" inside it; AESCipherManager still has key1
    GettableAction got = (GettableAction) encryptionCodec.decode(encoded);
    assertEquals(key, got.getKey());
    assertEquals(value, got.getValue());
  }

  @Test
  public void testDecodeWithMissingTokenThrowsAssertionError() {
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(true);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY)).thenReturn(TOKEN1 + TOKEN_KEY_DELIMITER + testKey1);
    EncryptionCodec encryptionCodec = new EncryptionCodecImpl(mockConfig, new ActionCodecImpl(objectManager));
    register(encryptionCodec);
    ByteBuffer[] encoded = encryptionCodec.encode(putAction(-1L));

    // Decoder only has TOKEN2 — TOKEN1 key is absent
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(true);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY)).thenReturn(TOKEN2 + TOKEN_KEY_DELIMITER + testKey2);
    EncryptionCodec encryptionCodec1 = new EncryptionCodecImpl(mockConfig, new ActionCodecImpl(objectManager));
    register(encryptionCodec1);
    Action decoded = encryptionCodec1.decode(encoded); // decode produces a LazyDecryptingGettableAction

    // Decryption is lazy — it happens on getKey()/getValue()
    GettableAction got = (GettableAction) decoded;
    assertThrows(AssertionError.class, got::getKey);
  }

  private PutAction putAction(long invalidatedLsn) {
    return new PutAction(objectManager, null, identifier.duplicate(), key.duplicate(),
        value.duplicate(), invalidatedLsn);
  }

  private CompactionAction compactionAction(long lsn) {
    return new CompactionAction(objectManager, new SimpleObjectManagerEntry<>(identifier.duplicate(),
        key.duplicate(), value.duplicate(), lsn));
  }

  private RemoveAction removeAction() {
    return new RemoveAction(objectManager, null, identifier.duplicate(), key.duplicate(), true);
  }

  private static ByteBuffer buf(String s) {
    return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
  }

  private static ByteBuffer flatten(ByteBuffer[] buffers) {
    int total = Arrays.stream(buffers).mapToInt(ByteBuffer::remaining).sum();
    ByteBuffer out = ByteBuffer.allocate(total);
    for (ByteBuffer b : buffers) out.put(b.duplicate());
    out.flip();
    return out;
  }

  private ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> plainCodec() {
    ActionCodecImpl actionCodec = new ActionCodecImpl(objectManager);
    register(actionCodec);
    return actionCodec;
  }

  private void register(ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> codec) {
    MapActions.registerActions(MAP_COLLECTION_ID, codec);
    TransactionActions.registerActions(TRANSACTION_COLLECTION_ID, codec);
    CompactionActions.registerActions(COMPACTION_COLLECTION_ID, codec);
  }

}

// Made with Bob
