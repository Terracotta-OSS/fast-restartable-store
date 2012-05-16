/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.hamcrest.core.CombinableMatcher;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.collection.IsIn.isIn;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNull.nullValue;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

/**
 *
 * @author Chris Dennis
 */
public abstract class ObjectManagerTest {
  
  protected abstract <I, K, V> ObjectManager<I, K, V> createObjectManager();

  private ObjectManagerEntry<String, String, String> entry(String id, String key,
                                                           String value, long lsn) {
    return new SimpleObjectManagerEntry<String, String, String>(id, key, value, lsn);
  }

  @Test
  public void basicPutTest() {
    ObjectManager<String, String, String> objMgr = createObjectManager();

    assertThat(objMgr.getLowestLsn(), is(-1L));
    assertThat(objMgr.acquireCompactionEntry(Long.MAX_VALUE), nullValue());

    objMgr.put("foo", "bar", "baz", 1);

    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(1L));
    ObjectManagerEntry<String, String, String> entry = objMgr.acquireCompactionEntry(Long.MAX_VALUE);
    assertThat(entry, is(entry("foo", "bar", "baz", 1)));
    objMgr.releaseCompactionEntry(entry);
    assertThat(objMgr.getLsn("foo", "bar"), is(1L));

    objMgr.put("foo", "bat", "baz", 2);

    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(1L));
    assertThat(objMgr.acquireCompactionEntry(Long.MAX_VALUE), CombinableMatcher.<ObjectManagerEntry<String, String, String>>either(
            is(entry)).or(is(entry("foo", "bat", "baz", 2))));
    assertThat(objMgr.getLsn("foo", "bar"), is(1L));
    assertThat(objMgr.getLsn("foo", "bat"), is(2L));
  }

  @Test
  public void basicRemoveTest() {
    ObjectManager<String, String, String> objMgr = createObjectManager();

    assertThat(objMgr.getLowestLsn(), is(-1L));
    assertThat(objMgr.acquireCompactionEntry(Long.MAX_VALUE), nullValue());

    objMgr.put("foo", "bar", "baz", 1);

    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(1L));
    ObjectManagerEntry<String, String, String> entry = objMgr.acquireCompactionEntry(
            Long.MAX_VALUE);
    assertThat(entry, is(entry("foo", "bar", "baz", 1)));
    objMgr.releaseCompactionEntry(entry);
    assertThat(objMgr.getLsn("foo", "bar"), is(1L));

    objMgr.remove("foo", "bar");

    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(1L));
    assertThat(objMgr.acquireCompactionEntry(Long.MAX_VALUE), nullValue());
  }

  @Test
  public void basicUpdateTest() {
    ObjectManager<String, String, String> objMgr = createObjectManager();

    assertThat(objMgr.getLowestLsn(), is(-1L));
    assertThat(objMgr.acquireCompactionEntry(Long.MAX_VALUE), nullValue());

    objMgr.put("foo", "bar", "baz", 1);

    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(1L));
    ObjectManagerEntry<String, String, String> entry = objMgr.acquireCompactionEntry(Long.MAX_VALUE);
    assertThat(entry, is(entry("foo", "bar", "baz", 1)));
    assertThat(objMgr.getLsn("foo", "bar"), is(1L));

    objMgr.updateLsn(entry, 2L);

    objMgr.releaseCompactionEntry(entry);

    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(2L));
    entry = objMgr.acquireCompactionEntry(Long.MAX_VALUE);
    assertThat(entry, is(entry("foo", "bar", "baz", 2L)));
    assertThat(objMgr.getLsn("foo", "bar"), is(2L));
  }

  @Test
  public void basicDeleteTest() {
    ObjectManager<String, String, String> objMgr = createObjectManager();

    assertThat(objMgr.getLowestLsn(), is(-1L));
    assertThat(objMgr.acquireCompactionEntry(Long.MAX_VALUE), nullValue());

    objMgr.put("foo", "bar", "baz", 1);

    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(1L));
    ObjectManagerEntry<String, String, String> entry = objMgr.acquireCompactionEntry(Long.MAX_VALUE);
    assertThat(entry, is(entry("foo", "bar", "baz", 1L)));
    assertThat(objMgr.getLsn("foo", "bar"), is(1L));
    objMgr.releaseCompactionEntry(entry);

    objMgr.put("foo", "bat", "baz", 2);

    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(1L));
    entry = objMgr.acquireCompactionEntry(Long.MAX_VALUE);
    assertThat(entry, CombinableMatcher.<ObjectManagerEntry<String, String, String>>either(
            is(entry("foo", "bar", "baz", 1L))).or(
            is(entry("foo", "bat", "baz", 2L))));
    assertThat(objMgr.getLsn("foo", "bar"), is(1L));
    assertThat(objMgr.getLsn("foo", "bat"), is(2L));
    objMgr.releaseCompactionEntry(entry);

    objMgr.delete("foo");

    //TODO what should be the assertion here?
    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(2L));
    assertThat(objMgr.acquireCompactionEntry(Long.MAX_VALUE), nullValue());
  }

  @Test
  public void basicReplayTest() {
    ObjectManager<String, String, String> objMgr = createObjectManager();

    objMgr.replayPut("foo", "bar", "baz", 1);

    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(1L));
    assertThat(objMgr.acquireCompactionEntry(Long.MAX_VALUE), is(entry("foo", "bar", "baz", 1)));
    assertThat(objMgr.getLsn("foo", "bar"), is(1L));
  }

  @Test
  public void basicCompactionTest() throws Exception {
    ObjectManager<String, String, String> objectManager = createObjectManager();

    objectManager.put("a", "b", "c", 123L);
    ObjectManagerEntry<String, String, String> entry =
            objectManager.acquireCompactionEntry(Long.MAX_VALUE);
    assertThat(entry, is(entry("a", "b", "c", 123L)));
    objectManager.releaseCompactionEntry(entry);

    assertThat(objectManager.acquireCompactionEntry(123L), nullValue());
  }

  @Test
  @Ignore
  /**
   * This test performs an illegal re-ordering hence it fails.
   */
  public void outOfOrderPutsTest() {
    ObjectManager<String, String, String> objMgr = createObjectManager();

    assertThat(objMgr.getLowestLsn(), is(-1L));
    assertThat(objMgr.acquireCompactionEntry(Long.MAX_VALUE), nullValue());

    objMgr.put("foo", "bar", "baz", 2);

    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(1L));
    ObjectManagerEntry<String, String, String> entry = objMgr.acquireCompactionEntry(Long.MAX_VALUE);
    assertThat(entry, is(entry("foo", "bar", "baz", 2)));
    assertThat(objMgr.getLsn("foo", "bar"), is(2L));
    objMgr.releaseCompactionEntry(entry);

    objMgr.put("foo", "bat", "baz", 1);

    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(1L));
    assertThat(objMgr.acquireCompactionEntry(Long.MAX_VALUE), CombinableMatcher.<ObjectManagerEntry<String, String, String>>either(
            is(entry("foo", "bar", "baz", 2))).or(
            is(entry("foo", "bat", "baz", 1))));
    assertThat(objMgr.getLsn("foo", "bar"), is(2L));
    assertThat(objMgr.getLsn("foo", "bat"), is(1L));
  }

  @Test
  public void predictableUpdateLowestLsnTest() {
    ObjectManager<String, String, String> objMgr = createObjectManager();
    Assume.assumeThat(objMgr, instanceOf(AbstractObjectManager.class));

    AbstractObjectManager<String, String, String> absObjMgr = (AbstractObjectManager<String, String, String>) objMgr;

    assertThat(objMgr.getLowestLsn(), is(-1L));
    assertThat(objMgr.acquireCompactionEntry(Long.MAX_VALUE), nullValue());

    absObjMgr.updateLowestLsn();
    assertThat(objMgr.getLowestLsn(), is(-1L));
    assertThat(objMgr.acquireCompactionEntry(Long.MAX_VALUE), nullValue());

    for (long i = 0L; i < 100L; i++) {
      objMgr.put("foo", Long.toString(i), "bar", i);

      assertThat(objMgr.getLowestLsn(), is(-1L));
      ObjectManagerEntry<String, String, String> entry = objMgr.acquireCompactionEntry(Long.MAX_VALUE);
      assertThat(Long.parseLong(entry.getKey()), lessThanOrEqualTo(i));
      objMgr.releaseCompactionEntry(entry);
      assertThat(objMgr.getLsn("foo", Long.toString(i)), is(i));
    }

    absObjMgr.updateLowestLsn();
    assertThat(objMgr.getLowestLsn(), is(0L));
    ObjectManagerEntry<String, String, String> entry = objMgr.acquireCompactionEntry(Long.MAX_VALUE);
    assertThat(Long.parseLong(entry.getKey()), lessThan(100L));
    objMgr.releaseCompactionEntry(entry);

    for (long i = 0L; i < 100L; i++) {
      objMgr.remove("foo", Long.toString(i));
      absObjMgr.updateLowestLsn();
      if (i == 99L) {
        assertThat(objMgr.getLowestLsn(), is(-1L));
        assertThat(objMgr.acquireCompactionEntry(Long.MAX_VALUE), nullValue());
      } else {
        assertThat(objMgr.getLowestLsn(), is(i + 1));
        entry = objMgr.acquireCompactionEntry(Long.MAX_VALUE);
        assertThat(Long.parseLong(entry.getKey()), CombinableMatcher.<Long>both(lessThan(100L)).and(greaterThan(i)));
        objMgr.releaseCompactionEntry(entry);
      }
    }
  }

  @Test
  public void randomUpdateLowestLsnTest() {
    ObjectManager<String, String, String> objMgr = createObjectManager();
    Assume.assumeThat(objMgr, instanceOf(AbstractObjectManager.class));

    AbstractObjectManager<String, String, String> absObjMgr = (AbstractObjectManager<String, String, String>) objMgr;

    assertThat(objMgr.getLowestLsn(), is(-1L));
    assertThat(objMgr.acquireCompactionEntry(Long.MAX_VALUE), nullValue());

    absObjMgr.updateLowestLsn();
    assertThat(objMgr.getLowestLsn(), is(-1L));
    assertThat(objMgr.acquireCompactionEntry(Long.MAX_VALUE), nullValue());

    List<Long> liveLsn = new LinkedList<Long>();
    for (long i = 0L; i < 100L; i++) {
      objMgr.put("foo", Long.toString(i), "bar", i);
      liveLsn.add(i);

      assertThat(objMgr.getLowestLsn(), is(-1L));
      ObjectManagerEntry<String, String, String> entry = objMgr.acquireCompactionEntry(Long.MAX_VALUE);
      assertThat(Long.parseLong(entry.getKey()), lessThanOrEqualTo(i));
      assertThat(objMgr.getLsn("foo", Long.toString(i)), is(i));
      objMgr.releaseCompactionEntry(entry);
    }

    absObjMgr.updateLowestLsn();
    assertThat(objMgr.getLowestLsn(), is(0L));
    ObjectManagerEntry<String, String, String> entry = objMgr.acquireCompactionEntry(Long.MAX_VALUE);
    assertThat(Long.parseLong(entry.getKey()), lessThan(100L));
    objMgr.releaseCompactionEntry(entry);

    long seed = System.nanoTime();
    System.err.println("randomUpdateLowestLsnTest using seed " + seed);
    Random rndm = new Random(seed);

    while (!liveLsn.isEmpty()) {
      long removalCandidate = liveLsn.remove(rndm.nextInt(liveLsn.size()));
      objMgr.remove("foo", Long.toString(removalCandidate));
      absObjMgr.updateLowestLsn();
      if (liveLsn.isEmpty()) {
        assertThat(objMgr.getLowestLsn(), is(-1L));
        assertThat(objMgr.acquireCompactionEntry(Long.MAX_VALUE), nullValue());
      } else {
        assertThat(objMgr.getLowestLsn(), is(liveLsn.get(0)));
        entry = objMgr.acquireCompactionEntry(Long.MAX_VALUE);
        assertThat(Long.parseLong(entry.getKey()), isIn(liveLsn));
        objMgr.releaseCompactionEntry(entry);
      }

    }
  }
  
  @Test 
  public void lowestLsnTest() {
      ObjectManager<Integer,String,String> testlow = createObjectManager();
      assertThat(testlow.getLowestLsn(),is(-1L));
      testlow.put(1, "test", "low", 2L);
      testlow.put(2, "test", "low", 2L);
      testlow.remove(2, "test");
      testlow.updateLowestLsn();
      assertThat(testlow.getLowestLsn(),is(2L));
        
  } 
}
