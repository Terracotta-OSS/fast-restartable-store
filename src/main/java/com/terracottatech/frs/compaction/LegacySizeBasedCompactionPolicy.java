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
