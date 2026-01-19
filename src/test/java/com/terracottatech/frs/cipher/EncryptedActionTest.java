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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.object.ObjectManager;

public class EncryptedActionTest {

    private Action mockAction;
    private CipherManager mockCipherManager;
    private ActionCodec mockCodec;
    private EncryptedAction encryptedAction;
    private ByteBuffer mockIV;

    @Before
    public void setUp() {
        mockAction = mock(Action.class);
        mockCipherManager = mock(CipherManager.class);
        mockCodec = mock(ActionCodec.class);
        mockIV = ByteBuffer.allocate(16);

        // Fill IV with some test data
        for (int i = 0; i < 16; i++) {
            mockIV.put((byte) i);
        }
        mockIV.flip();

        when(mockCipherManager.generateInitializationVector()).thenReturn(mockIV);

        encryptedAction = new EncryptedAction(mockAction, mockCipherManager);
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
        ByteBuffer[] originalPayload = new ByteBuffer[] {
            ByteBuffer.wrap("test payload".getBytes())
        };
        ByteBuffer[] encryptedPayload = new ByteBuffer[] {
            ByteBuffer.wrap("encrypted data".getBytes())
        };

        when(mockCodec.encode(mockAction)).thenReturn(originalPayload);
        when(mockCipherManager.encrypt(originalPayload, mockIV)).thenReturn(encryptedPayload);

        // Call the method under test
        ByteBuffer[] result = encryptedAction.getPayload(mockCodec);

        // Verify the result
        assertNotNull(result);
        assertEquals(3, result.length); // Header + IV + encrypted payload

        // Verify the header contains the correct IV length and payload length
        ByteBuffer header = result[0];
        header.rewind();
        assertEquals(mockIV.remaining(), header.getInt());
        assertEquals("encrypted data".getBytes().length, header.getInt());

        // Verify the IV is included
        assertEquals(mockIV, result[1]);

        // Verify the encrypted payload is included
        assertEquals(encryptedPayload[0], result[2]);

        // Verify the interactions
        verify(mockCodec).encode(mockAction);
        verify(mockCipherManager).encrypt(originalPayload, mockIV);
    }

    @Test
    public void testEncryptedActionFactory() {
        // Setup
        ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> mockObjectManager = mock(ObjectManager.class);
        ActionCodec mockFactoryCodec = mock(ActionCodec.class);

        // Create the factory
        EncryptedAction.EncryptedActionFactory factory =
            new EncryptedAction.EncryptedActionFactory(mockCipherManager);

        // Create test data
        ByteBuffer ivBuffer = ByteBuffer.allocate(16);
        for (int i = 0; i < 16; i++) {
            ivBuffer.put((byte) i);
        }
        ivBuffer.flip();

        ByteBuffer encryptedPayload = ByteBuffer.wrap("encrypted data".getBytes());
        ByteBuffer decryptedPayload = ByteBuffer.wrap("decrypted data".getBytes());

        // Create input buffers with header, IV, and encrypted payload
        ByteBuffer header = ByteBuffer.allocate(8);
        header.putInt(ivBuffer.remaining());
        header.putInt(encryptedPayload.remaining());
        header.flip();

        ByteBuffer[] inputBuffers = new ByteBuffer[] { header, ivBuffer, encryptedPayload };

        // Setup mock behavior
        when(mockCipherManager.decrypt(any(ByteBuffer.class), any(ByteBuffer.class)))
            .thenReturn(decryptedPayload);

        Action mockDecodedAction = mock(Action.class);
        when(mockFactoryCodec.decode(any(ByteBuffer[].class))).thenReturn(mockDecodedAction);

        // Call the method under test
        Action result = factory.create(mockObjectManager, mockFactoryCodec, inputBuffers);

        // Verify the result
        assertEquals(mockDecodedAction, result);

        // Verify the interactions
        verify(mockCipherManager).decrypt(any(ByteBuffer.class), any(ByteBuffer.class));
        verify(mockFactoryCodec).decode(any(ByteBuffer[].class));
    }

    @Test
    public void testEqualsAndHashCode() {
        // Create two EncryptedAction objects with the same delegate
        EncryptedAction action1 = new EncryptedAction(mockAction, mockCipherManager);
        EncryptedAction action2 = new EncryptedAction(mockAction, mockCipherManager);

        // Test equals
        assertEquals(action1, action2);

        // Test hashCode
        assertEquals(action1.hashCode(), action2.hashCode());

        // Create an EncryptedAction with a different delegate
        Action differentMockAction = mock(Action.class);
        EncryptedAction differentAction = new EncryptedAction(differentMockAction, mockCipherManager);

        // They should not be equal
        assertTrue(!action1.equals(differentAction));
    }
}

// Made with Bob
