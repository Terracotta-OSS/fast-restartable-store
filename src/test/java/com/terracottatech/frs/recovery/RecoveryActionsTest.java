/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.recovery;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionCodecImpl;
import com.terracottatech.frs.object.ObjectManager;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

/**
 * @author tim
 */
public class RecoveryActionsTest {
  private ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> codec;

  @Before
  public void setUp() throws Exception {
    codec = new ActionCodecImpl<ByteBuffer, ByteBuffer, ByteBuffer>(mock(ObjectManager.class));
    RecoveryActions.registerActions(0, codec);
  }

  @Test
  public void testEvictionAction() throws Exception {
    Set<Long> invalidated = new HashSet<Long>(Arrays.asList(1L, 2L, 3L));
    Action eviction = new RecoveryEvictionAction(invalidated);
    assertThat(codec.decode(codec.encode(eviction)), is(eviction));
  }
}
