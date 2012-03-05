/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.FilterRule;
import com.terracottatech.fastrestartablestore.messages.Action;

/**
 *
 * @author cdennis
 */
public class MockDeleteFilter<I> implements FilterRule {

  private final I id;
  
  public MockDeleteFilter(I id) {
    this.id = id;
  }
  
  @Override
  public boolean disallows(Action action) {
    if (action instanceof MockPutAction) {
      if (id.equals(((MockPutAction<?, ?, ?>) action).getId())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean allows(Action action) {
    return false;
  }
  
}
