/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.RestartStore;
import com.terracottatech.fastrestartablestore.TransactionContext;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;
import org.junit.Test;

/**
 *
 * @author cdennis
 */
public class MockTest {
  
  @Test
  public void testMock() {
    ObjectManager objectManager = new MockObjectManager();
    RestartStore mock = MockRestartStore.create(objectManager);
    
    TransactionContext<String, String> context = mock.createTransaction();
    context.put("foo", "bar");
    context.commit();
  }
}
