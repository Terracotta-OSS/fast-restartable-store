/*
 * Copyright (c) 2017-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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
package com.terracottatech.frs.io.nio;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by cschanck on 1/11/2017.
 */
public class WritingSegmentJumpListTest {

  @Test
  public void testEmpty() throws Exception {
    WritingSegmentJumpList jl = new WritingSegmentJumpList();
    assertThat(jl.size(), is(0));
    assertThat(jl.iterator().hasNext(), is(false));
  }

  @Test
  public void testSingle() throws Exception {
    WritingSegmentJumpList jl = new WritingSegmentJumpList(16);
    for (int i = 0; i < 15; i++) {
      jl.add(1000000l + i);
    }
    assertThat(jl.size(), is(15));
    assertThat(jl.iterator().hasNext(), is(true));
    long probe = 1000000;
    int cnt = 0;
    for (Long l : jl) {
      assertThat(l.longValue(), is(probe++));
      cnt++;
    }
    assertThat(cnt, is(15));
  }

  @Test
  public void testMany() throws Exception {
    WritingSegmentJumpList jl = new WritingSegmentJumpList(16);
    for (int i = 0; i < 10000; i++) {
      jl.add(1000000l + i);
    }
    assertThat(jl.size(), is(10000));
    assertThat(jl.iterator().hasNext(), is(true));
    long probe = 1000000;
    int cnt = 0;
    for (Long l : jl) {
      assertThat(l.longValue(), is(probe++));
      cnt++;
    }
    assertThat(cnt, is(10000));
  }


  @Test
  public void testClear() throws Exception {
    WritingSegmentJumpList jl = new WritingSegmentJumpList(16);
    for (int i = 0; i < 10000; i++) {
      jl.add(1000000l + i);
    }
    assertThat(jl.size(), is(10000));
    jl.clear();
    assertThat(jl.size(), is(0));
    assertThat(jl.iterator().hasNext(), is(false));

  }
}