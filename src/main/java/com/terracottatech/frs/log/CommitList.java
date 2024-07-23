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
package com.terracottatech.frs.log;

import java.util.concurrent.Future;

/**
 *
 * @author mscott
 */
public interface CommitList extends Iterable<LogRecord> {
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
    Future<Void> getWriteFuture();
}
