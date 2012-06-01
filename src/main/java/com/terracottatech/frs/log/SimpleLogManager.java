/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.IOManager;

/**
 *
 * Simple LogManager with a single daemon thread for IO operations
 * 
 * 
 * @author mscott
 */
@Deprecated
public class SimpleLogManager extends StagingLogManager {

    public SimpleLogManager(Signature check, CommitList list, IOManager io) {
        super(check, list, io);
    }

    public SimpleLogManager(IOManager io) {
        super(io);
    }
}
