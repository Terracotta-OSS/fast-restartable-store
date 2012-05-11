/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

/**
 *
 * @author mscott
 */
public class LogWriteError extends Error {

    /**
     * Creates a new instance of
     * <code>LogWriteError</code> without detail message.
     */
    public LogWriteError() {
    }

    /**
     * Constructs an instance of
     * <code>LogWriteError</code> with the specified detail message.
     *
     * @param msg the detail message.
     */
    public LogWriteError(String msg) {
        super(msg);
    }
    
    public LogWriteError(Exception ioe) {
        super(ioe);
    }
}
