/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

/**
 *
 * @author mscott
 */
public class HeaderException extends Exception {

    /**
     * Creates a new instance of
     * <code>HeaderException</code> without detail message.
     */
    public HeaderException() {
    }

    /**
     * Constructs an instance of
     * <code>HeaderException</code> with the specified detail message.
     *
     * @param msg the detail message.
     */
    public HeaderException(String msg) {
        super(msg);
    }
}
