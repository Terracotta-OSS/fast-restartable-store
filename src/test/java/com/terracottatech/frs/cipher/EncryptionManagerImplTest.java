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
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.config.FrsProperty;

import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collections;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import org.junit.Before;
import org.junit.Test;

import static com.terracottatech.frs.cipher.EncryptionManagerImpl.TOKEN_KEY_DELIMITER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EncryptionManagerImplTest {

  private Configuration mockConfig;
  private ActionCodec mockActionCodec;
  private String testKey1;
  private String testKey2;
  private static final String TOKEN1 = "token1";
  private static final String TOKEN2 = "token2";

  @Before
  public void setUp() throws Exception {
    mockConfig = mock(Configuration.class);
    mockActionCodec = mock(ActionCodec.class);

    // Generate test keys
    KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
    keyGenerator.init(256);
    SecretKey secretKey1 = keyGenerator.generateKey();
    testKey1 = Base64.getEncoder().encodeToString(secretKey1.getEncoded());

    SecretKey secretKey2 = keyGenerator.generateKey();
    testKey2 = Base64.getEncoder().encodeToString(secretKey2.getEncoded());
  }

  @Test
  public void testConstructorWithEncryptionDisabled() {
    // Setup config for disabled encryption
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(false);

    // Create manager
    EncryptionManager manager = new EncryptionManagerImpl(mockConfig, mockActionCodec);

    // Verify it uses NoEncryptionHandler
    assertNotNull("Manager should not be null", manager);
    assertTrue("Previous token should not be present when encryption is disabled",
        manager.getPreviousTokens().isEmpty());
  }

  @Test
  public void testConstructorWithEncryptionEnabledSingleKey() {
    // Setup config for enabled encryption with single key
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(true);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY)).thenReturn(TOKEN1 + TOKEN_KEY_DELIMITER + testKey1);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_OLD_TOKENS_AND_KEYS)).thenReturn(null);

    // Create manager
    EncryptionManager manager = new EncryptionManagerImpl(mockConfig, mockActionCodec);

    // Verify
    assertNotNull("Manager should not be null", manager);
    assertTrue("Should be using the new token", manager.isUsingEncKey(TOKEN1));
    assertTrue("Previous token should not be present with single key",
        manager.getPreviousTokens().isEmpty());
  }

  @Test
  public void testConstructorWithEncryptionEnabledTwoKeys() {
    // Setup config for enabled encryption with two keys
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(true);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY)).thenReturn(TOKEN2 + TOKEN_KEY_DELIMITER + testKey2);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_OLD_TOKENS_AND_KEYS)).thenReturn(TOKEN1 + TOKEN_KEY_DELIMITER + testKey1);

    // Create manager
    EncryptionManager manager = new EncryptionManagerImpl(mockConfig, mockActionCodec);

    // Verify
    assertNotNull("Manager should not be null", manager);
    assertTrue("Should be using the new token", manager.isUsingEncKey(TOKEN2));
    assertTrue("Should be using the old token", manager.isUsingEncKey(TOKEN1));
    assertFalse("Previous token should be present with two keys",
        manager.getPreviousTokens().isEmpty());
    assertEquals("Previous token should be TOKEN1", TOKEN1, manager.getPreviousTokens().get(0));
  }

  @Test
  public void testGetPreviousTokenWithEncryptionDisabled() {
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(false);

    EncryptionManager manager = new EncryptionManagerImpl(mockConfig, mockActionCodec);

    assertTrue("Previous token should not be present when encryption is disabled",
        manager.getPreviousTokens().isEmpty());
  }

  @Test
  public void testIsUsingEncKeyWithEncryptionEnabled() {
    // Setup config for enabled encryption
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(true);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY)).thenReturn(TOKEN2 + TOKEN_KEY_DELIMITER + testKey2);

    EncryptionManager manager = new EncryptionManagerImpl(mockConfig, mockActionCodec);

    assertTrue("Should return true for existing token", manager.isUsingEncKey(TOKEN2));
    assertFalse("Should return false for non-existing token", manager.isUsingEncKey("nonExistentToken"));
  }

  @Test
  public void testIsUsingEncKeyWithEncryptionDisabled() {
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(false);

    EncryptionManager manager = new EncryptionManagerImpl(mockConfig, mockActionCodec);

    assertFalse("Should return false for any token when encryption is disabled",
        manager.isUsingEncKey(TOKEN1));
  }

  @Test
  public void testAddWithEncryptionEnabled() {
    // Setup config for enabled encryption with single key
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(true);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY)).thenReturn(TOKEN1 + TOKEN_KEY_DELIMITER + testKey1);

    EncryptionManager manager = new EncryptionManagerImpl(mockConfig, mockActionCodec);

    // Add a new token
    manager.add(TOKEN2, Base64.getDecoder().decode(testKey2));

    // Verify the new token was added
    assertTrue("Should be using the newly added token", manager.isUsingEncKey(TOKEN2));
    assertTrue("Should still be using the original token", manager.isUsingEncKey(TOKEN1));
  }

  @Test
  public void testAddWithEncryptionDisabled() {
    // Setup config for disabled encryption
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(false);

    EncryptionManager manager = new EncryptionManagerImpl(mockConfig, mockActionCodec);

    // Add a token (should enable encryption)
    manager.add(TOKEN1, Base64.getDecoder().decode(testKey1));

    // Verify the token was added and encryption is now enabled
    assertTrue("Should be using the added token", manager.isUsingEncKey(TOKEN1));
  }

  @Test
  public void testRemoveWithEncryptionEnabled() {
    // Setup config for enabled encryption with two keys
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(true);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY)).thenReturn(TOKEN2 + TOKEN_KEY_DELIMITER + testKey2);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_OLD_TOKENS_AND_KEYS)).thenReturn(TOKEN1 + TOKEN_KEY_DELIMITER + testKey1);

    EncryptionManager manager = new EncryptionManagerImpl(mockConfig, mockActionCodec);

    // Verify both tokens exist
    assertTrue("TOKEN1 should exist before removal", manager.isUsingEncKey(TOKEN1));
    assertTrue("TOKEN2 should exist before removal", manager.isUsingEncKey(TOKEN2));

    // Remove TOKEN1
    manager.remove(Collections.singletonList(TOKEN1));

    // Verify TOKEN1 was removed
    assertFalse("TOKEN1 should not exist after removal", manager.isUsingEncKey(TOKEN1));
    assertTrue("TOKEN2 should still exist after removing TOKEN1", manager.isUsingEncKey(TOKEN2));
  }

  @Test
  public void testRemoveWithEncryptionDisabled() {
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(false);

    EncryptionManager manager = new EncryptionManagerImpl(mockConfig, mockActionCodec);

    assertThrows(UnsupportedOperationException.class, () -> manager.remove(Collections.singletonList(TOKEN1)));
  }

  @Test
  public void testConvertWithEncryptionEnabled() {
    // Setup config for enabled encryption
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(true);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY)).thenReturn(TOKEN1 + TOKEN_KEY_DELIMITER + testKey1);
    
    EncryptionManager manager = new EncryptionManagerImpl(mockConfig, mockActionCodec);

    // Create a mock action
    Action mockAction = mock(GettableAction.class);

    // Convert the action to EncryptedAction
    Action convertedAction = manager.convert(mockAction);

    // Verify conversion occurred (the actual conversion logic is in the handler)
    assertNotNull("Converted action should not be null", convertedAction);
    assertTrue(convertedAction instanceof EncryptedGettableAction);
  }

  @Test
  public void testConvertWithEncryptionDisabled() {
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(false);

    EncryptionManager manager = new EncryptionManagerImpl(mockConfig, mockActionCodec);

    // Create a mock action
    Action mockAction = mock(Action.class);

    // Convert the action
    Action convertedAction = manager.convert(mockAction);

    // Verify the action is returned as-is when encryption is disabled
    assertNotNull("Converted action should not be null", convertedAction);
    assertEquals("Action should be returned as-is when encryption is disabled",
        mockAction, convertedAction);
  }

  @Test
  public void testMultipleAddOperations() throws NoSuchAlgorithmException {
    // Setup config for enabled encryption
    when(mockConfig.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE)).thenReturn(true);
    when(mockConfig.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY)).thenReturn(TOKEN1 + TOKEN_KEY_DELIMITER + testKey1);

    EncryptionManager manager = new EncryptionManagerImpl(mockConfig, mockActionCodec);

    // Add multiple tokens
    manager.add(TOKEN2, Base64.getDecoder().decode(testKey2));

    KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
    keyGenerator.init(256);
    SecretKey secretKey = keyGenerator.generateKey();
    String testKey3 = Base64.getEncoder().encodeToString(secretKey.getEncoded());
    
    manager.add("token3", Base64.getDecoder().decode(testKey3));

    // Verify all tokens are present
    assertTrue("TOKEN1 should be present", manager.isUsingEncKey(TOKEN1));
    assertTrue("TOKEN2 should be present", manager.isUsingEncKey(TOKEN2));
    assertTrue("token3 should be present", manager.isUsingEncKey("token3"));
  }
}

// Made with Bob
