package com.terracottatech.frs.config;

public enum FrsProperty {
	
  IO_CHECKSUM("io.checksum", Type.STRING, "ADLER32"),
  IO_COMMIT_QUEUE_SIZE("io.commitQueueSize", Type.INTEGER, 1024),
  IO_RECOVERY_QUEUE_SIZE("io.recoveryQueueSize", Type.INTEGER, 1024),
  IO_COMMITLIST("io.commitList", Type.STRING, "ATOMIC"),
  IO_WAIT("io.wait", Type.INTEGER, 20),
  
  IO_NIO_SEGMENT_SIZE("io.nio.segmentSize", Type.LONG, 16L * 1024 * 1024),
  IO_NIO_MEMORY_SIZE("io.nio.memorySize", Type.LONG, ((Long) IO_NIO_SEGMENT_SIZE.defaultValue()) * 4),
  IO_NIO_BUFFER_BUILDER("io.nio.bufferBuilder", Type.STRING, null),
  
  RECOVERY_COMPRESSED_SKIP_SET("recovery.compressedSkipSet", Type.BOOLEAN, true),
  RECOVERY_MIN_THREAD_COUNT("recovery.minThreadCount", Type.INTEGER, 1),
  RECOVERY_MAX_THREAD_COUNT("recovery.maxThreadCount", Type.INTEGER, 64),
  RECOVERY_MAX_QUEUE_LENGTH("recovery.maxQueueLength", Type.INTEGER, 1000),
	
  COMPACTOR_POLICY("compactor.policy", Type.STRING, "LSNGapCompactionPolicy"),
  COMPACTOR_THROTTLE_AMOUNT("compactor.throttleAmount", Type.LONG,  1000L),
  COMPACTOR_RUN_INTERVAL("compactor.runInterval", Type.LONG, 300L),
  COMPACTOR_START_THRESHOLD("compactor.startThreshold", Type.INTEGER, 50000),

  COMPACTOR_LSNGAP_MIN_LOAD("compactor.lsnGap.minLoad", Type.DOUBLE, 0.30),
  COMPACTOR_LSNGAP_MAX_LOAD("compactor.lsnGap.maxLoad", Type.DOUBLE, 0.60),

  COMPACTOR_SIZEBASED_THRESHOLD("compactor.sizeBased.threshold", Type.DOUBLE, 0.50),
  COMPACTOR_SIZEBASED_AMOUNT("compactor.sizeBased.amount", Type.DOUBLE, 0.05);
  
  private final String property;
  private final Type type;
  private final Object defaultValue;

  <T> FrsProperty(String property, Type type, T defaultValue) {
    this.property = property;
    this.type = type;
    this.defaultValue = defaultValue;
  }

  public String property() {
    return property;
  }

  public Object convert(String string) {
    return type.convert(string);
  }

  public Object defaultValue() {
    return defaultValue;
  }	
	
  enum Type {
    STRING {
      @Override
      String convert(String string) {
        return string;
      }
    },
    BOOLEAN {
      @Override
      Boolean convert(String string) {
        return Boolean.valueOf(string);
      }
    },
    INTEGER {
      @Override
      Integer convert(String string) {
        return Integer.valueOf(string);
      }
    },
    LONG {
      @Override
      Long convert(String string) {
        return Long.valueOf(string);
      }
    },
    FLOAT {
      @Override
      Float convert(String string) {
        return Float.valueOf(string);
      }
    },
    DOUBLE {
      @Override
      Double convert(String string) {
        return Double.valueOf(string);
      }
    };

    abstract Object convert(String string);
  }
}
