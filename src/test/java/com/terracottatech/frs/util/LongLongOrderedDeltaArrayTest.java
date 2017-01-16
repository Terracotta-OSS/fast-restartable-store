package com.terracottatech.frs.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class LongLongOrderedDeltaArrayTest {

  @Test
  public void testConstructor() {
    LongLongOrderedDeltaArray omi = new LongLongOrderedDeltaArray(16);
    assertThat(omi.getChunkSize(), is(16));
    assertThat(omi.getShiftAmount(), is(4));
    assertThat(omi.getIndexMask(), is(15));
    LongLongOrderedDeltaArray omi2 = new LongLongOrderedDeltaArray(14);
    assertThat(omi2.getChunkSize(), is(16));
    assertThat(omi2.getShiftAmount(), is(4));
    assertThat(omi2.getIndexMask(), is(15));
  }

  @Test
  public void testErrorAdd() {
    LongLongOrderedDeltaArray omi = new LongLongOrderedDeltaArray(16);
    omi.append(-1, -1);
    try {
      omi.append((long) (Integer.MAX_VALUE) + 1l, 0);
      Assert.fail();
    } catch (IllegalStateException e) {
    }
    try {
      omi.append(0, (long) (Integer.MAX_VALUE) + 1l);
      Assert.fail();
    } catch (IllegalStateException e) {
    }
  }

  @Test
  public void testAddsGets() throws Exception {
    LongLongOrderedDeltaArray omi = new LongLongOrderedDeltaArray(16);
    assertThat(omi.isEmpty(), is(true));
    assertThat(omi.size(), is(0));
    for (int i = 0; i < 100; i++) {
      omi.append(i, Long.MAX_VALUE / 4 + i);
    }
    assertThat(omi.isEmpty(), is(false));
    assertThat(omi.size(), is(100));
    for (int i = 0; i < 100; i++) {
      assertThat(omi.get(i).getKey(), is((long) i));
      assertThat(omi.get(i).getValue(), is(Long.MAX_VALUE / 4 + i));
      assertThat(omi.getKey(i), is((long) i));
      assertThat(omi.getValue(i), is(Long.MAX_VALUE / 4 + i));
    }
    for (int i = 0; i < 100; i++) {
      omi.update(i, i, Long.MAX_VALUE / 4 + 2 * i);
    }
    assertThat(omi.size(), is(100));
    for (int i = 0; i < 100; i++) {
      assertThat(omi.get(i).getKey(), is((long) i));
      assertThat(omi.get(i).getValue(), is(Long.MAX_VALUE / 4 + 2 * i));
      assertThat(omi.getKey(i), is((long) i));
      assertThat(omi.getValue(i), is(Long.MAX_VALUE / 4 + 2 * i));
    }
  }

  @Test
  public void testSearch() throws Exception {
    LongLongOrderedDeltaArray omi = new LongLongOrderedDeltaArray(16);
    for (int i = 0; i < 100; i++) {
      omi.append(i * 2, Long.MAX_VALUE / 4 + i * 2);
    }
    Random r = new Random(0);
    for (int cnt = 0; cnt < 200; cnt++) {
      int p = r.nextInt(200);
      int idx = omi.binarySearch(p);
      if ((p & 0x01) == 0) {
        // even, should be present
        assertThat(idx, greaterThanOrEqualTo(0));
        LongLongOrderedDeltaArray.LongLongEntry ent = omi.get(idx);
        assertThat(ent.getKey(), is((long) p));
        assertThat(ent.getValue(), is(Long.MAX_VALUE / 4 + p));
      } else {
        // odd, should miss.
        assertThat(idx, lessThan(0));
        idx = ~idx;
        LongLongOrderedDeltaArray.LongLongEntry ent = omi.get(idx);
        assertThat(ent.getKey(), is((long) p + 1));
      }
    }
  }

}