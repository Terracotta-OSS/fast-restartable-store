/*
 * Copyright (c) 2013-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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

import com.terracottatech.frs.io.HeapBufferSource;
import java.nio.ByteBuffer;
import java.util.Random;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.mockito.ArgumentMatchers;
import org.mockito.Matchers;

import static org.mockito.AdditionalMatchers.or;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 *
 * @author mscott
 */
public class MarkerIndexTest {
  
  HeapBufferSource source;
  MarkerIndex markers;
  long[] list;
  
  public MarkerIndexTest() {
  }
  
  @BeforeClass
  public static void setUpClass() {
  }
  
  @AfterClass
  public static void tearDownClass() {
  }
  
  @Before
  public void setUp() {
    source = spy(new HeapBufferSource(Long.MAX_VALUE));
    markers = new MarkerIndex(source);
    list = randomizeJumpList(8192);
    markers.append(list);
  }
  
  @After
  public void tearDown() {
  }
  
      
    protected long[] randomizeJumpList(int size) {
        long[] gen = new long[size];
        Random r = new Random();
        long base = 0;
        for (int x=0;x<size;x++) {
            base += r.nextInt(8192);
            gen[x] = base;
        }
        return gen;
    }

  /**
   * Test of position method, of class MarkerIndex.
   */
  @Test
  public void testPosition() {
    System.out.println("position");
    int index = 0;
    MarkerIndex instance = markers;
    for ( long i : list ) {
      long result = instance.position(index++);
      assertEquals(i, result);
    }
  }

  /**
   * Test of mark method, of class MarkerIndex.
   */
  @Test
  public void testMark() {
    System.out.println("mark");
    int index = 0;
    long marker = 100;
    MarkerIndex instance = markers;
    for ( int x=0;x<list.length;x++) {
      instance.cache(x,marker++);
    }
    marker = 100;
    for ( int x=0;x<list.length;x++) {
      assertEquals(marker++,instance.mark(x));
    }
  }

  /**
   * Test of size method, of class MarkerIndex.
   */
  @Test
  public void testSize() {
    System.out.println("size");
    assertEquals(list.length, markers.size());
  }

  /**
   * Test of close method, of class MarkerIndex.
   */
  @Test
  public void testClose() throws Exception {
    System.out.println("close");
    markers.close();
    verify(source).returnBuffer(or(any(ByteBuffer.class), isNull()));
  }
  
   @Test
  public void testAppend() throws Exception {
    System.out.println("append");
    markers.append(randomizeJumpList(6000));
    assertEquals(8192 + 6000, markers.size());
    System.out.println(markers);
  }
   
  
}
