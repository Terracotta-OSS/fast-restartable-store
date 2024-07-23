/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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
package com.terracottatech.frs.compaction;

import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.object.ObjectManager;

import java.io.IOException;

public class LegacySizeBasedCompactionPolicy extends SizeBasedCompactionPolicy {
  public LegacySizeBasedCompactionPolicy(IOManager ioManager, ObjectManager<?, ?, ?> objectManager, Configuration configuration) {
    super(ioManager, objectManager, configuration);
  }

  protected float getRatio(ObjectManager<?, ?, ?> objectManager, IOManager ioManager) {
    try {
      return (float)(objectManager.sizeInBytes() * 1.0d / ioManager.getStatistics().getLiveSize());
    } catch (IOException e) {
      throw new RuntimeException("Failed to get log size.", e);
    }
  }
}
