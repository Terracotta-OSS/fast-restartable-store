/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object;

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
  
  @Test
  public void basicPutTest() {
    ObjectManager<String, String, String> objMgr = createObjectManager();

    assertThat(objMgr.getLowestLsn(), is(-1L));
    assertThat(objMgr.getCompactionKey(), nullValue());
    
    objMgr.put("foo", "bar", "baz", 1);
    
    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(1L));
    assertThat(objMgr.getCompactionKey(), is((CompleteKey<String, String>) new SimpleCompleteKey<String, String>("foo", "bar")));
    assertThat(objMgr.getLsn("foo", "bar"), is(1L));

    objMgr.put("foo", "bat", "baz", 2);
    
    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(1L));
    assertThat(objMgr.getCompactionKey(), CombinableMatcher.<CompleteKey<String, String>>either(
            is((CompleteKey<String, String>) new SimpleCompleteKey<String, String>("foo", "bar"))).or(
            is((CompleteKey<String, String>) new SimpleCompleteKey<String, String>("foo", "bat"))));
    assertThat(objMgr.getLsn("foo", "bar"), is(1L));
    assertThat(objMgr.getLsn("foo", "bat"), is(2L));
  }
  
  @Test
  public void basicRemoveTest() {
    ObjectManager<String, String, String> objMgr = createObjectManager();

    assertThat(objMgr.getLowestLsn(), is(-1L));
    assertThat(objMgr.getCompactionKey(), nullValue());
    
    objMgr.put("foo", "bar", "baz", 1);
    
    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(1L));
    assertThat(objMgr.getCompactionKey(), is((CompleteKey<String, String>) new SimpleCompleteKey<String, String>("foo", "bar")));
    assertThat(objMgr.getLsn("foo", "bar"), is(1L));

    objMgr.remove("foo", "bar");
    
    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(1L));
    assertThat(objMgr.getCompactionKey(), nullValue());
  }
  
  @Test
  public void basicReplaceTest() {
    ObjectManager<String, String, String> objMgr = createObjectManager();

    assertThat(objMgr.getLowestLsn(), is(-1L));
    assertThat(objMgr.getCompactionKey(), nullValue());
    
    objMgr.put("foo", "bar", "baz", 1);
    
    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(1L));
    assertThat(objMgr.getCompactionKey(), is((CompleteKey<String, String>) new SimpleCompleteKey<String, String>("foo", "bar")));
    assertThat(objMgr.getLsn("foo", "bar"), is(1L));

    assertThat(objMgr.replaceLsn("foo", "bar", 2L), is("baz"));
    
    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(2L));
    assertThat(objMgr.getCompactionKey(), is((CompleteKey<String, String>) new SimpleCompleteKey<String, String>("foo", "bar")));
    assertThat(objMgr.getLsn("foo", "bar"), is(2L));
  }
  
  @Test
  public void basicDeleteTest() {
    ObjectManager<String, String, String> objMgr = createObjectManager();

    assertThat(objMgr.getLowestLsn(), is(-1L));
    assertThat(objMgr.getCompactionKey(), nullValue());
    
    objMgr.put("foo", "bar", "baz", 1);
    
    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(1L));
    assertThat(objMgr.getCompactionKey(), is((CompleteKey<String, String>) new SimpleCompleteKey<String, String>("foo", "bar")));
    assertThat(objMgr.getLsn("foo", "bar"), is(1L));

    objMgr.put("foo", "bat", "baz", 2);
    
    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(1L));
    assertThat(objMgr.getCompactionKey(), CombinableMatcher.<CompleteKey<String, String>>either(
            is((CompleteKey<String, String>) new SimpleCompleteKey<String, String>("foo", "bar"))).or(
            is((CompleteKey<String, String>) new SimpleCompleteKey<String, String>("foo", "bat"))));
    assertThat(objMgr.getLsn("foo", "bar"), is(1L));
    assertThat(objMgr.getLsn("foo", "bat"), is(2L));

    objMgr.delete("foo");

    //TODO what should be the assertion here?
    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(2L));
    assertThat(objMgr.getCompactionKey(), nullValue());
  }
  
  @Test
  public void basicReplayTest() {
    ObjectManager<String, String, String> objMgr = createObjectManager();

    objMgr.replayPut("foo", "bar", "baz", 1);
    
    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(1L));
    assertThat(objMgr.getCompactionKey(), is((CompleteKey<String, String>) new SimpleCompleteKey<String, String>("foo", "bar")));
    assertThat(objMgr.getLsn("foo", "bar"), is(1L));
  }
  
  @Test
  @Ignore
  /**
   * This test performs an illegal re-ordering hence it fails.
   */
  public void outOfOrderPutsTest() {
    ObjectManager<String, String, String> objMgr = createObjectManager();

    assertThat(objMgr.getLowestLsn(), is(-1L));
    assertThat(objMgr.getCompactionKey(), nullValue());
    
    objMgr.put("foo", "bar", "baz", 2);
    
    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(1L));
    assertThat(objMgr.getCompactionKey(), is((CompleteKey<String, String>) new SimpleCompleteKey<String, String>("foo", "bar")));
    assertThat(objMgr.getLsn("foo", "bar"), is(2L));

    objMgr.put("foo", "bat", "baz", 1);
    
    assertThat(objMgr.getLowestLsn(), lessThanOrEqualTo(1L));
    assertThat(objMgr.getCompactionKey(), CombinableMatcher.<CompleteKey<String, String>>either(
            is((CompleteKey<String, String>) new SimpleCompleteKey<String, String>("foo", "bar"))).or(
            is((CompleteKey<String, String>) new SimpleCompleteKey<String, String>("foo", "bat"))));
    assertThat(objMgr.getLsn("foo", "bar"), is(2L));
    assertThat(objMgr.getLsn("foo", "bat"), is(1L));
  }

  @Test
  public void predictableUpdateLowestLsnTest() {
    ObjectManager<String, String, String> objMgr = createObjectManager();
    Assume.assumeThat(objMgr, instanceOf(AbstractObjectManager.class));

    AbstractObjectManager<String, String, String> absObjMgr = (AbstractObjectManager<String, String, String>) objMgr;
    
    assertThat(objMgr.getLowestLsn(), is(-1L));
    assertThat(objMgr.getCompactionKey(), nullValue());

    absObjMgr.updateLowestLsn();
    assertThat(objMgr.getLowestLsn(), is(-1L));
    assertThat(objMgr.getCompactionKey(), nullValue());

    for (long i = 0L; i < 100L; i++) {
      objMgr.put("foo", Long.toString(i), "bar", i);

      assertThat(objMgr.getLowestLsn(), is(-1L));
      assertThat(Long.parseLong(objMgr.getCompactionKey().getKey()), lessThanOrEqualTo(i));
      assertThat(objMgr.getLsn("foo", Long.toString(i)), is(i));
    }

    absObjMgr.updateLowestLsn();
    assertThat(objMgr.getLowestLsn(), is(0L));
    assertThat(Long.parseLong(objMgr.getCompactionKey().getKey()), lessThan(100L));
    
    for (long i = 0L; i < 100L; i++) {
      objMgr.remove("foo", Long.toString(i));
      absObjMgr.updateLowestLsn();
      if (i == 99L) {
        assertThat(objMgr.getLowestLsn(), is(-1L));
        assertThat(objMgr.getCompactionKey(), nullValue());
      } else {
        assertThat(objMgr.getLowestLsn(), is(i + 1));
        assertThat(Long.parseLong(objMgr.getCompactionKey().getKey()), CombinableMatcher.<Long>both(lessThan(100L)).and(greaterThan(i)));
      }
    }
  }

  @Test
  public void randomUpdateLowestLsnTest() {
    ObjectManager<String, String, String> objMgr = createObjectManager();
    Assume.assumeThat(objMgr, instanceOf(AbstractObjectManager.class));

    AbstractObjectManager<String, String, String> absObjMgr = (AbstractObjectManager<String, String, String>) objMgr;
    
    assertThat(objMgr.getLowestLsn(), is(-1L));
    assertThat(objMgr.getCompactionKey(), nullValue());

    absObjMgr.updateLowestLsn();
    assertThat(objMgr.getLowestLsn(), is(-1L));
    assertThat(objMgr.getCompactionKey(), nullValue());

    List<Long> liveLsn = new LinkedList<Long>();
    for (long i = 0L; i < 100L; i++) {
      objMgr.put("foo", Long.toString(i), "bar", i);
      liveLsn.add(i);

      assertThat(objMgr.getLowestLsn(), is(-1L));
      assertThat(Long.parseLong(objMgr.getCompactionKey().getKey()), lessThanOrEqualTo(i));
      assertThat(objMgr.getLsn("foo", Long.toString(i)), is(i));
    }

    absObjMgr.updateLowestLsn();
    assertThat(objMgr.getLowestLsn(), is(0L));
    assertThat(Long.parseLong(objMgr.getCompactionKey().getKey()), lessThan(100L));
    
    long seed = System.nanoTime();
    System.err.println("randomUpdateLowestLsnTest using seed " + seed);
    Random rndm = new Random(seed);

    while (!liveLsn.isEmpty()) {
      long removalCandidate = liveLsn.remove(rndm.nextInt(liveLsn.size()));
      objMgr.remove("foo", Long.toString(removalCandidate));
      absObjMgr.updateLowestLsn();
      if (liveLsn.isEmpty()) {
        assertThat(objMgr.getLowestLsn(), is(-1L));
        assertThat(objMgr.getCompactionKey(), nullValue());
      } else {
        assertThat(objMgr.getLowestLsn(), is(liveLsn.get(0)));
        assertThat(Long.parseLong(objMgr.getCompactionKey().getKey()), isIn(liveLsn));
      }
      
    }
    for (long i = 0L; i < 100L; i++) {
    }
  }
}
