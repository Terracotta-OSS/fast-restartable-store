/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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
import junit.framework.Assert;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import org.mockito.ArgumentMatchers;
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
    doReturn(true).when(delegate).filter(any(Action.class), anyLong(), ArgumentMatchers.eq(true));
    doReturn(false).when(delegate).filter(any(Action.class), anyLong(), ArgumentMatchers.eq(false));
    objectManager = mock(ObjectManager.class);
    filter = new DeleteFilter(delegate);
    mapActionFactory = new MapActionFactory(objectManager, compactor);
  }

  @Test
  public void testFilterDeleteAction() throws Exception {
    assertThat(filter.filter(mapActionFactory.delete(1), 2, false), is(true));
    assertThat(filter.filter(mapActionFactory.put(1, 2, 3), 1, false), is(true));
    verify(delegate).filter(any(Action.class), eq(1L), eq(true));
    assertThat(filter.filter(mapActionFactory.put(2, 3, 3), 0, false), is(false));
    verify(delegate).filter(any(PutAction.class), eq(0L), eq(false));
  }

  @Test
  public void testPassthrough() throws Exception {
    Action bogusAction = mock(Action.class);
    assertThat(filter.filter(mapActionFactory.delete(1), 2, false), is(true));
    assertThat(filter.filter(bogusAction, 1, false), is(false));
    verify(delegate).filter(eq(bogusAction), anyLong(), anyBoolean());
  }

  @Test
  public void testByteBufferSet() throws Exception {
    Set<ByteBuffer> buffers = new HashSet<ByteBuffer>();
    buffers.add(TestUtils.byteBufferWithInt(1));
    assertThat(buffers.contains(TestUtils.byteBufferWithInt(1)), is(true));
  }
  
  
  @Test
  public void testBufferStomping() throws Exception {
    DeleteAction action = mock(DeleteAction.class);
    ByteBuffer buffer = TestUtils.byteBufferWithInt(1);
    when(action.getId()).thenReturn(buffer);
    filter.filter(action, 1, false);
    buffer.putInt(0,0);
    GettableAction item = mock(GettableAction.class);
    when(item.getIdentifier()).thenReturn(TestUtils.byteBufferWithInt(1));
    Assert.assertTrue(filter.filter(item, 1, false));
    
  }
}
