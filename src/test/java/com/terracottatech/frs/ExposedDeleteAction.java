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
