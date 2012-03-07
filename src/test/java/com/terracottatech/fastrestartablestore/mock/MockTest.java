/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.IOManager;
import com.terracottatech.fastrestartablestore.RestartStore;
import com.terracottatech.fastrestartablestore.Transaction;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.IsEqual.equalTo;
import org.junit.Assert;

import org.junit.Test;

/**
 *
 * @author cdennis
 */
public class MockTest {
  
  @Test
  public void testMock() {
    IOManager ioManager = new MockIOManager();
    
    Map<Long, Map<String, String>> outsideWorld = new HashMap<Long, Map<String, String>>();
    RestartStore<Long, String, String> mock = MockRestartStore.create(new MockObjectManager(outsideWorld), ioManager);
    
    Transaction<Long, String, String> context = mock.beginTransaction();
    context.put(1L, "far", "bar");
    outsideWorld.put(1L, new HashMap<String, String>());
    outsideWorld.get(1L).put("far", "bar");
    context.commit();
    
    context = mock.beginTransaction();
    context.put(1L, "foo", "baz");
    outsideWorld.get(1L).put("foo", "baz");
    context.commit();
    
    context = mock.beginTransaction();
    context.remove(1L, "foo");
    outsideWorld.get(1L).remove("foo");
    context.commit();

    context = mock.beginTransaction();
    context.put(1L, "bar", "baz");
    outsideWorld.get(1L).put("bar", "baz");
    context.commit();
    
    context = mock.beginTransaction();
    context.put(1L, "foo", "baz");
//    outsideWorld.get(1L).put("foo", "baz");

    context = mock.beginTransaction();
    context.remove(1L, "bar");
//    outsideWorld.get(1L).remove("bar");
    
    System.out.println("XXXXX CRASHING HERE XXXXX");
    
    //crash here - all knowledge lost - except IOManager
    
    Map<Long, Map<String, String>>restoredWorld = new HashMap<Long, Map<String, String>>();
    MockRestartStore.create(new MockObjectManager(restoredWorld), ioManager);
    Assert.assertThat(restoredWorld, equalTo(outsideWorld));
  }
}
