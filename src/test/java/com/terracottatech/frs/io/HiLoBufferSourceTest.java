/*
 * All content copyright (c) 2014 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.frs.io;

import java.nio.ByteBuffer;
import org.hamcrest.number.OrderingComparison;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.hamcrest.number.OrderingComparison.lessThan;

import org.mockito.AdditionalMatchers;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import static org.mockito.Matchers.anyInt;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 *
 * @author mscott
 */
public class HiLoBufferSourceTest extends BufferSourceTest {
  

  public HiLoBufferSourceTest() {
  }

  @Override
  public BufferSource getBufferSource() {
    return new HiLoBufferSource(1024, 8*1024*1024, 64*1024*1024);
  }
  
  @BeforeClass
  public static void setUpClass() {
  }
  
  @AfterClass
  public static void tearDownClass() {
  }
  
  @Before
  @Override
  public void setUp() {
    super.setUp();
  }
  
  @After
  public void tearDown() {
  }
  
  @Test
  public void testBufferReturn() {
    HiLoBufferSource src = (HiLoBufferSource)getBufferSource();
// test splitting
    ByteBuffer buf = src.getBuffer(1);
//  internal implementation detail 
    assertThat(buf.getInt(0),greaterThanOrEqualTo(0));
    src.returnBuffer(buf);
//  test slab
    buf = src.getBuffer(2048);
//  internal implementation detail 
   assertThat(buf.getInt(0),lessThan(0));
    src.returnBuffer(buf);
//  test slab
    buf = src.getBuffer(8*1024*1024+1);
//  internal implementation detail 
   assertThat(buf.getInt(0),lessThan(0)); 
    src.returnBuffer(buf);
  }
  
  @Test
  public void testProperSectioningAndReturn() {
    final SplittingBufferSource lo = Mockito.mock(SplittingBufferSource.class);
    Mockito.when(lo.getBuffer(anyInt())).then(new Answer<Object>() {

      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return ByteBuffer.allocate((Integer)invocation.getArguments()[0] + SplittingBufferSource.HEADERSZ);
      }
    });
    final SLABBufferSource hi = Mockito.mock(SLABBufferSource.class);
    Mockito.when(hi.getBuffer(anyInt())).then(new Answer<Object>() {

      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return ByteBuffer.allocate((Integer)invocation.getArguments()[0] + SLABBufferSource.HEADERSZ);
      }
    });
    Mockito.when(hi.getSlabSize()).thenReturn(8*1024*1024);
    final CachingBufferSource cache = Mockito.mock(CachingBufferSource.class);
    Mockito.when(cache.getBuffer(anyInt())).thenReturn(null);
    
    HiLoBufferSource src = new HiLoBufferSource(2048, new HiLoBufferSource.BufferProvider() {

      @Override
      public SplittingBufferSource getSmallSource() {
        return lo;
      }

      @Override
      public SLABBufferSource getLargeSource() {
        return hi;
      }

      @Override
      public CachingBufferSource getLargeCache() {
        return cache;
      }
    });
    
    ByteBuffer buf = src.getBuffer(1024);
    Mockito.verify(lo).getBuffer(Matchers.eq(1024));
    src.returnBuffer(buf);
    Mockito.verify(lo).returnBuffer(Matchers.eq(buf));
    buf = src.getBuffer(4096);
    Mockito.verify(hi).getBuffer(Matchers.eq(4096));
    src.returnBuffer(buf);
    Mockito.verify(hi).returnBuffer(Matchers.eq(buf));
    buf = src.getBuffer(10*1024*1024);
    Mockito.verify(cache).getBuffer(AdditionalMatchers.geq(10*1024*1024));
    assertNotNull(buf);
    src.returnBuffer(buf);
    Mockito.verify(cache).returnBuffer(Matchers.eq(buf));
  }
  
}
