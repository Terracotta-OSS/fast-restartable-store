/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import java.util.concurrent.Future;

/**
 *
 * @author mscott
 */
public interface CommitList extends Iterable<LogRecord>,Future<Void> {
    boolean append(LogRecord record, boolean sync);
    boolean close(long lsn);
    void waitForContiguous() throws InterruptedException;
    CommitList next();
    boolean isSyncRequested();
    boolean isEmpty();
    long getEndLsn();
    long getBaseLsn();
    void written();
    void exceptionThrown(Exception exp);
    CommitList create(long baseLsn);
}
