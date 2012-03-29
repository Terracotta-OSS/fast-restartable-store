/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.mock;

import static org.hamcrest.core.IsEqual.equalTo;

import java.util.HashMap;
import java.util.Map;

import com.terracottatech.frs.mock.io.MockIOManager;
import com.terracottatech.frs.mock.object.MockObjectManager;
import org.junit.Assert;
import org.junit.Test;

import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.Transaction;

/**
 *
 * @author cdennis
 */
public class MockTest {
  
  @Test
  public void testMock() throws Exception {
    IOManager ioManager = new MockIOManager();
    
    Map<Long, Map<String, String>> outsideWorld = new HashMap<Long, Map<String, String>>();
    MockRestartStore mock = MockRestartStore.create(new MockObjectManager<Long, String, String>(outsideWorld), ioManager);
    
    Transaction<Long, String, String> transaction = mock.beginTransaction();
    transaction.put(1L, "far", "bar");
    outsideWorld.put(1L, new HashMap<String, String>());
    outsideWorld.get(1L).put("far", "bar");
    transaction.commit();
    
    transaction = mock.beginTransaction();
    transaction.put(1L, "foo", "baz");
    outsideWorld.get(1L).put("foo", "baz");
    transaction.commit();
    
    transaction = mock.beginTransaction();
    transaction.remove(1L, "foo");
    outsideWorld.get(1L).remove("foo");
    mock.compact();
    transaction.commit();

    transaction = mock.beginTransaction();
    transaction.put(1L, "bar", "baz");
    outsideWorld.get(1L).put("bar", "baz");
    mock.compact();
    transaction.commit();
    
    transaction = mock.beginTransaction();
    transaction.put(1L, "foo", "bazzab");
//    outsideWorld.get(1L).put("foo", "baz");

    transaction = mock.beginTransaction();
    transaction.remove(1L, "bar");
//    outsideWorld.get(1L).remove("bar");

    transaction = mock.beginTransaction();
    transaction.put(2L, "foo", "bar");
    transaction.put(2L, "baz", "boo");
//    outsideWorld.put(2L, new HashMap<String, String>());
//    outsideWorld.get(2L).put("foo", "bar");
//    outsideWorld.get(2L).put("baz", "boo");
    transaction.commit();

    transaction = mock.beginTransaction();
    transaction.put(2L, "foo", "baz");
//    outsideWorld.get(2L).put("foo", "baz");
    transaction.commit();

    transaction = mock.beginTransaction();
    transaction.delete(2L);
    outsideWorld.remove(2L);
    transaction.commit();
    
    System.out.println("XXXXX CRASHING HERE XXXXX");

    //crash here - all knowledge lost - except IOManager
    
    Map<Long, Map<String, String>>restoredWorld = new HashMap<Long, Map<String, String>>();
    MockRestartStore.create(new MockObjectManager<Long, String, String>(restoredWorld), ioManager);
    Assert.assertThat(restoredWorld, equalTo(outsideWorld));
  }
}
