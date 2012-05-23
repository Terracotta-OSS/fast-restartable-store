/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

/**
 *
 * @author mscott
 */
public class OutOfDirectMemoryError extends Error {

    /**
     * Creates a new instance of
     * <code>OutOfDirectMemoryError</code> without detail message.
     */
    public OutOfDirectMemoryError() {
    }

    /**
     * Constructs an instance of
     * <code>OutOfDirectMemoryError</code> with the specified detail message.
     *
     * @param msg the detail message.
     */
    public OutOfDirectMemoryError(String msg) {
        super(msg);
    }
}
