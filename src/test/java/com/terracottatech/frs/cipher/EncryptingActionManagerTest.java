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

import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.log.LogRecord;

public class EncryptingActionManagerTest {

    private ActionManager mockDelegate;
    private CipherManager mockCipherManager;
    private EncryptingActionManager encryptingActionManager;
    private Action mockAction;
    private Future<Void> mockFuture;
    private LogRecord mockLogRecord;

    @Before
    public void setUp() {
        mockDelegate = mock(ActionManager.class);
        mockCipherManager = mock(CipherManager.class);
        mockAction = mock(Action.class);
        mockFuture = mock(Future.class);
        mockLogRecord = mock(LogRecord.class);

        encryptingActionManager = new EncryptingActionManager(mockDelegate, mockCipherManager);
    }

    @Test
    public void testHappened() {
        // Setup
        when(mockDelegate.happened(any(EncryptedAction.class))).thenReturn(mockFuture);

        // Call the method under test
        Future<Void> result = encryptingActionManager.happened(mockAction);

        // Verify the result
        assertSame(mockFuture, result);

        // Verify that the delegate was called with an EncryptedAction
        ArgumentCaptor<EncryptedAction> actionCaptor = ArgumentCaptor.forClass(EncryptedAction.class);
        verify(mockDelegate).happened(actionCaptor.capture());

        // Verify that the EncryptedAction contains our original action
        EncryptedAction encryptedAction = actionCaptor.getValue();
        assertEquals(mockAction, encryptedAction.getDelegate());
    }

    @Test
    public void testSyncHappened() {
        // Setup
        when(mockDelegate.syncHappened(any(EncryptedAction.class))).thenReturn(mockFuture);

        // Call the method under test
        Future<Void> result = encryptingActionManager.syncHappened(mockAction);

        // Verify the result
        assertSame(mockFuture, result);

        // Verify that the delegate was called with an EncryptedAction
        ArgumentCaptor<EncryptedAction> actionCaptor = ArgumentCaptor.forClass(EncryptedAction.class);
        verify(mockDelegate).syncHappened(actionCaptor.capture());

        // Verify that the EncryptedAction contains our original action
        EncryptedAction encryptedAction = actionCaptor.getValue();
        assertEquals(mockAction, encryptedAction.getDelegate());
    }

    @Test
    public void testExtractWithEncryptedAction() {
        // Setup
        EncryptedAction mockEncryptedAction = mock(EncryptedAction.class);
        when(mockEncryptedAction.getDelegate()).thenReturn(mockAction);
        when(mockDelegate.extract(mockLogRecord)).thenReturn(mockEncryptedAction);

        // Call the method under test
        Action result = encryptingActionManager.extract(mockLogRecord);

        // Verify the result
        assertSame(mockAction, result);

        // Verify the interactions
        verify(mockDelegate).extract(mockLogRecord);
        verify(mockEncryptedAction).getDelegate();
    }

    @Test
    public void testExtractWithNonEncryptedAction() {
        // Setup
        when(mockDelegate.extract(mockLogRecord)).thenReturn(mockAction);

        // Call the method under test
        Action result = encryptingActionManager.extract(mockLogRecord);

        // Verify the result
        assertSame(mockAction, result);

        // Verify the interactions
        verify(mockDelegate).extract(mockLogRecord);
    }

    @Test
    public void testPause() {
        // Call the method under test
        encryptingActionManager.pause();

        // Verify the delegate was called
        verify(mockDelegate).pause();
    }

    @Test
    public void testResume() {
        // Call the method under test
        encryptingActionManager.resume();

        // Verify the delegate was called
        verify(mockDelegate).resume();
    }

    @Test
    public void testBarrierAction() {
        // Setup
        when(mockDelegate.barrierAction()).thenReturn(mockLogRecord);

        // Call the method under test
        LogRecord result = encryptingActionManager.barrierAction();

        // Verify the result
        assertSame(mockLogRecord, result);

        // Verify the delegate was called
        verify(mockDelegate).barrierAction();
    }
}

// Made with Bob
