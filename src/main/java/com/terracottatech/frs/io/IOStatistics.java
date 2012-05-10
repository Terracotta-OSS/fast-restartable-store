/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

/**
 *
 * @author mscott
 */
public interface IOStatistics {
    long getTotalAvailable();
    long getTotalUsed();
    long getTotalWritten();
    long getTotalRead();
    long getLiveSize();
    long getExpiredSize();
}
