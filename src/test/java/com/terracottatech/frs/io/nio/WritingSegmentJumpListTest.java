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