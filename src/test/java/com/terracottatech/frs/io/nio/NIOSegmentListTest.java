/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.util.JUnitTestFolder;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ListIterator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Rule;

/**
 *
 * @author mscott
 */
public class NIOSegmentListTest {
  @Rule
  public JUnitTestFolder tempFolder = new JUnitTestFolder();
  
  public NIOSegmentListTest() {
  }
  
  @BeforeClass
  public static void setUpClass() {
  }
  
  @AfterClass
  public static void tearDownClass() {
  }
  
  @Before
  public void setUp() {
  }
  
  @After
  public void tearDown() {
  }

  public static int writeSomeData(byte[] data, File f) throws IOException {
    FileOutputStream fos = new FileOutputStream(f);
    fos.write(data);
    fos.close();
    return data.length;
  }
  /**
   * Test of getTotalSize method, of class NIOSegmentList.
   */
  @Test
  public void testGetTotalSize() throws Exception {
    System.out.println("getTotalSize");
    NIOSegmentList list = new NIOSegmentList(tempFolder.newFolder());
    File f = list.appendFile();
    int len = writeSomeData("some data".getBytes(),f);
    assertEquals("written equals count", len, list.getTotalSize());
    f = list.appendFile();
    assertEquals("written equals count", len, list.getTotalSize());
    len += writeSomeData("even more data".getBytes(),f);
    assertEquals("written equals count", len, list.getTotalSize());
  }

  /**
   * Test of appendFile method, of class NIOSegmentList.
   */
  @Test
  public void testAppendFile() throws Exception {
    System.out.println("getTotalSize");
    NIOSegmentList list = new NIOSegmentList(tempFolder.newFolder());
    File f = list.appendFile();
    assertEquals("added file", 1, list.size());
  }

  /**
   * Test of isEmpty method, of class NIOSegmentList.
   */
  @Test
  public void testIsEmpty() throws Exception {
    System.out.println("isEmpty");
    NIOSegmentList list = new NIOSegmentList(tempFolder.newFolder());
    assertTrue(list.isEmpty());
  }

  /**
   * Test of setReadPosition method, of class NIOSegmentList.
   */
  @Test
  public void testScanning() throws Exception {
    System.out.println("setReadPosition");
    NIOSegmentList list = new NIOSegmentList(tempFolder.newFolder());
    File f = list.appendFile();
    new FileOutputStream(f).close();
    File f1 = list.appendFile();
    new FileOutputStream(f1).close();
    File f2 = list.appendFile();
    new FileOutputStream(f2).close();
    
    list.setReadPosition(0);
    File test = list.nextReadFile(Direction.FORWARD);
    assertEquals(f, test);
    assertEquals(0,list.getSegmentPosition());
    test = list.nextReadFile(Direction.FORWARD);
    assertEquals(f1, test);
    assertEquals(1,list.getSegmentPosition());
    test = list.nextReadFile(Direction.FORWARD);
    assertEquals(f2, test);
    assertEquals(2,list.getSegmentPosition());
    test = list.nextReadFile(Direction.FORWARD);
    assertNull(test);
    assertEquals(3,list.getSegmentPosition());
    test = list.nextReadFile(Direction.REVERSE);
    assertEquals(f2,test);
    assertEquals(2,list.getSegmentPosition());
    test = list.nextReadFile(Direction.REVERSE);
    assertEquals(f1,test);
    assertEquals(1,list.getSegmentPosition());
    test = list.nextReadFile(Direction.REVERSE);
    assertEquals(f,test);    
    assertEquals(0,list.getSegmentPosition());
    test = list.nextReadFile(Direction.REVERSE);
    assertNull(test);    
    assertEquals(-1,list.getSegmentPosition());
    
    list.nextReadFile(Direction.FORWARD);
    list.nextReadFile(Direction.FORWARD);
    list.removeFilesFromTail();
    assertEquals(f1, list.getBeginningFile());
    assertEquals(1, list.getSegmentPosition());
    assertEquals(1, list.getBeginningSegmentId());
    list.removeFilesFromHead();
    assertEquals(f1, list.getEndFile());
  }



  /**
   * Test of getCurrentReadFile method, of class NIOSegmentList.
   */
  @Test
  public void testGetCurrentReadFile() throws Exception {
    System.out.println("getCurrentReadFile");
    NIOSegmentList list = new NIOSegmentList(tempFolder.newFolder());
    File f = list.appendFile();
    new FileOutputStream(f).close();
    File f1 = list.appendFile();
    new FileOutputStream(f1).close();
    File f2 = list.appendFile();
    new FileOutputStream(f2).close();
    
    assertNull(list.getCurrentReadFile());
    list.nextReadFile(Direction.FORWARD);
    assertEquals(f,list.getCurrentReadFile());
  }

  /**
   * Test of getEndFile method, of class NIOSegmentList.
   */
  @Test
  public void testGetEndFile() throws Exception {
    System.out.println("getEndFile");
    NIOSegmentList list = new NIOSegmentList(tempFolder.newFolder());
    File f = list.appendFile();
    new FileOutputStream(f).close();
    File f1 = list.appendFile();
    new FileOutputStream(f1).close();
    File f2 = list.appendFile();
    new FileOutputStream(f2).close();
    
    assertEquals(f2,list.getEndFile());
  }

  /**
   * Test of getFile method, of class NIOSegmentList.
   */
  @Test
  public void testGetFile() throws IOException {
    System.out.println("getEndFile");
    NIOSegmentList list = new NIOSegmentList(tempFolder.newFolder());
    File f = list.appendFile();
    new FileOutputStream(f).close();
    File f1 = list.appendFile();
    new FileOutputStream(f1).close();
    File f2 = list.appendFile();
    new FileOutputStream(f2).close();
    
    assertEquals(f2,list.getFile(2));
  }
  
  @Test
  public void testGetWhileDelete() throws Throwable {
    System.out.println("getWhileDelete");
    NIOSegmentList list = new NIOSegmentList(tempFolder.newFolder());
    File f = list.appendFile();
    new FileOutputStream(f).close();
    File f1 = list.appendFile();
    new FileOutputStream(f1).close();
    File f2 = list.appendFile();
    new FileOutputStream(f2).close();
    
    assertEquals(f2,list.getFile(2));
    
    ListIterator it = list.listIterator(2);
    it.remove();
    
    assertNull(list.get(2));   // should not throw
    it.remove();  // does not throw exception
  }
  
}
