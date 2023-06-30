

package com.terracottatech.frs.config;

public enum FrsProperty {
  IO_CHECKSUM("io.checksum", Type.STRING, "ADLER32"),
  IO_RANDOM_ACCESS("io.randomAccess", Type.BOOLEAN, false),
  IO_COMMIT_QUEUE_SIZE("io.commitQueueSize", Type.INTEGER, 1024),
  IO_RECOVERY_QUEUE_SIZE("io.recoveryQueueSize", Type.INTEGER, 16),
  IO_COMMITLIST("io.commitList", Type.STRING, "ATOMIC"),
  IO_WAIT("io.wait", Type.INTEGER, 200),
  IO_DISABLE_SYNC("io.disableSync", Type.BOOLEAN, false),
  
  IO_NIO_SEGMENT_SIZE("io.nio.segmentSize", Type.LONG, 512L * 1024 * 1024),
  IO_NIO_RECOVERY_MEMORY_SIZE("io.nio.recoveryMemorySize", Type.LONG, -1L),
  IO_NIO_POOL_MEMORY_SIZE("io.nio.memorySize", Type.LONG, 64L * 1024 * 1024),
  IO_NIO_RANDOM_ACCESS_MEMORY_SIZE("io.nio.randomAccessMemorySize", Type.LONG, -1L),
  IO_NIO_FILECACHE_MAX("io.nio.maxOpenFiles", Type.INTEGER, 32 * 1024),
  IO_NIO_MEMORY_TIMEOUT("io.nio.memoryTimeout", Type.LONG, 0L),
  IO_NIO_BUFFER_BUILDER("io.nio.bufferBuilder", Type.STRING, null),
  IO_NIO_ACCESS_METHOD("io.nio.accessMethod", Type.STRING, "STREAM"),
  IO_NIO_BUFFER_SOURCE("io.nio.bufferSource", Type.STRING, "HILO"),
  
  RECOVERY_COMPRESSED_SKIP_SET("recovery.compressedSkipSet", Type.BOOLEAN, true),
  RECOVERY_REPLAY_PER_BATCH_SIZE("recovery.replayPerBatchSize", Type.INTEGER, 512),
  RECOVERY_REPLAY_TOTAL_BATCH_SIZE_MAX("recovery.replayTotalBatchSize", Type.INTEGER, 2048),
  
  COMPACTOR_POLICY("compactor.policy", Type.STRING, "SizeBasedCompactionPolicy"),
  COMPACTOR_THROTTLE_AMOUNT("compactor.throttleAmount", Type.LONG,  1000L),
  COMPACTOR_RUN_INTERVAL("compactor.runInterval", Type.LONG, 300L),
  COMPACTOR_START_THRESHOLD("compactor.startThreshold", Type.INTEGER, 50000),
  COMPACTOR_RETRY_INTERVAL("compactor.retryInterval", Type.LONG, 600L),

  COMPACTOR_LSNGAP_MIN_LOAD("compactor.lsnGap.minLoad", Type.DOUBLE, 0.50),
  COMPACTOR_LSNGAP_MAX_LOAD("compactor.lsnGap.maxLoad", Type.DOUBLE, 0.60),
  COMPACTOR_LSNGAP_WINDOW_SIZE("compactor.lsnGap.windowSize", Type.INTEGER, 20),

  COMPACTOR_SIZEBASED_THRESHOLD("compactor.sizeBased.threshold", Type.DOUBLE, 0.50),
  COMPACTOR_SIZEBASED_AMOUNT("compactor.sizeBased.amount", Type.DOUBLE, 0.05),

  STORE_MAX_PAUSE_TIME_IN_MILLIS("store.maxPauseTimeInMillis", Type.INTEGER, 1000);

  private static final String SYSTEM_PROPERTY_PREFIX = "com.tc.frs.";

  private final String property;
  private final Type type;
  private final Object defaultValue;

  <T> FrsProperty(String property, Type type, T defaultValue) {
    this.property = property;
    this.type = type;
    this.defaultValue = defaultValue;
  }

  public String shortName() {
    return property;
  }

  public String property() {
    return SYSTEM_PROPERTY_PREFIX + shortName();
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
