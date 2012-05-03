/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.compaction.Compactor;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.recovery.Filter;
import com.terracottatech.frs.util.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author tim
 */
public class DeleteFilterTest {
  private Filter<Action>                                    delegate;
  private DeleteFilter                                      filter;
  private ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private Compactor compactor;
  private MapActionFactory mapActionFactory;

  @Before
  public void setUp() throws Exception {
    compactor = mock(Compactor.class);
    delegate = mock(Filter.class);
    doReturn(true).when(delegate).filter(any(Action.class), anyLong());
    objectManager = mock(ObjectManager.class);
    filter = new DeleteFilter(delegate);
    mapActionFactory = new MapActionFactory(objectManager, compactor);
  }

  @Test
  public void testFilterDeleteAction() throws Exception {
    assertThat(filter.filter(mapActionFactory.delete(1), 2), is(true));
    assertThat(filter.filter(mapActionFactory.put(1, 2, 3), 1), is(true));
    verify(delegate, never()).filter(any(Action.class), anyLong());
    assertThat(filter.filter(mapActionFactory.put(2, 3, 3), 0), is(true));
    verify(delegate).filter(any(PutAction.class), anyLong());
  }

  @Test
  public void testPassthrough() throws Exception {
    Action bogusAction = mock(Action.class);
    assertThat(filter.filter(mapActionFactory.delete(1), 2), is(true));
    assertThat(filter.filter(bogusAction, 1), is(true));
    verify(delegate).filter(eq(bogusAction), anyLong());
  }

  @Test
  public void testByteBufferSet() throws Exception {
    Set<ByteBuffer> buffers = new HashSet<ByteBuffer>();
    buffers.add(TestUtils.byteBufferWithInt(1));
    assertThat(buffers.contains(TestUtils.byteBufferWithInt(1)), is(true));
  }
}
