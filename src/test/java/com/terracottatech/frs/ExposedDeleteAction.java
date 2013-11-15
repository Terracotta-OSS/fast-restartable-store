/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.terracottatech.frs;

import com.terracottatech.frs.compaction.Compactor;
import com.terracottatech.frs.object.ObjectManager;
import java.nio.ByteBuffer;

/**
 *
 * @author mscott
 */
public class ExposedDeleteAction extends DeleteAction {

  public ExposedDeleteAction(ObjectManager<ByteBuffer, ?, ?> objectManager, Compactor compactor, ByteBuffer id, boolean recovery) {
    super(objectManager, compactor, id, recovery);
  }

  @Override
  public void dispose() {
    super.dispose(); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public ByteBuffer getId() {
    return super.getId(); //To change body of generated methods, choose Tools | Templates.
  }
  
  
}
