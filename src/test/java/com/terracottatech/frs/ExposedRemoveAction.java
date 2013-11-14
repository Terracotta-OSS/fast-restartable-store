/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.compaction.Compactor;
import com.terracottatech.frs.object.ObjectManager;
import java.nio.ByteBuffer;

/**
 *
 * @author mscott
 */
public class ExposedRemoveAction extends RemoveAction {

  public ExposedRemoveAction(ObjectManager<ByteBuffer, ByteBuffer, ?> objectManager, Compactor compactor, ByteBuffer id, ByteBuffer key, boolean recovery) {
    super(objectManager, compactor, id, key, recovery);
  }

  @Override
  public void dispose() {
    super.dispose(); //To change body of generated methods, choose Tools | Templates.
  }
  
}
