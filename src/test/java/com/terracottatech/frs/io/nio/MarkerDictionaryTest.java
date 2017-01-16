package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.util.LongLongOrderedDeltaArray;
import com.terracottatech.frs.util.LongLongOrderedDeltaArray.LongLongEntry;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.*;
import static org.junit.Assert.assertThat;

/**
 * Created by cschanck on 1/16/2017.
 */
public class MarkerDictionaryTest {

  @Test
  public void testConstructor() {
    MarkerDictionary md=new MarkerDictionary();
  }


  @Test
  public void testAddsGetsUpdates() throws Exception {
    MarkerDictionary md = new MarkerDictionary();
    assertThat(md.isEmpty(), is(true));
    assertThat(md.size(), is(0));
    for (int i = 0; i < 10000; i++) {
      md.append(i, Long.MAX_VALUE / 4 + i);
    }
    assertThat(md.isEmpty(), is(false));
    assertThat(md.size(), is(10000));
    for (int i = 0; i < 10000; i++) {
      assertThat(md.get(i).getValue(), is(Long.MAX_VALUE / 4 + i));
    }
    for (int i = 0; i < 10000; i++) {
      md.replace(i, Long.MAX_VALUE / 4 + 2 * i);
    }
    assertThat(md.size(), is(10000));
    for (int i = 0; i < 10000; i++) {
      assertThat(md.get(i).getValue(), is(Long.MAX_VALUE / 4 + 2 * i));
    }
  }


  @Test
  public void testSearch() throws Exception {
    MarkerDictionary md = new MarkerDictionary();
    for (int i = 0; i < 1000; i++) {
      md.append(i * 2, Long.MAX_VALUE / 4 + i * 2);
    }
    Random r = new Random(0);

    // ceiling
    for (int cnt = 0; cnt < 1000; cnt++) {
      int p = r.nextInt(1000);
      LongLongEntry ent = md.ceilingEntry(p);
      if ((p & 0x01) == 0) {
        // even, should be present
        assertThat(ent.getKey(), is((long) p));
        assertThat(ent.getValue(), is(Long.MAX_VALUE / 4 + p));
      } else {
        // odd, should miss.
        assertThat(ent.getKey(), is((long) p+1));
      }
    }

    // floor
    for (int cnt = 0; cnt < 1000; cnt++) {
      int p = r.nextInt(1000);
      LongLongEntry ent = md.floorEntry(p);
      if ((p & 0x01) == 0) {
        // even, should be present
        assertThat(ent.getKey(), is((long) p));
        assertThat(ent.getValue(), is(Long.MAX_VALUE / 4 + p));
      } else {
        // odd, should miss.
        assertThat(ent.getKey(), is((long) p-1));
      }
    }

    // ends
    LongLongEntry ent1 = md.floorEntry(-1l);
    assertThat(ent1, nullValue());
    LongLongEntry ent2 = md.floorEntry(2000l);
    assertThat(ent2.getKey(), is(1998l));
    LongLongEntry ent3 = md.ceilingEntry(-1l);
    assertThat(ent3.getKey(), is(0l));
    LongLongEntry ent4 = md.ceilingEntry(2000l);
    assertThat(ent4, nullValue());

  }

}