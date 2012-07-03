/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/**
 *
 * @author mscott
 */
public enum SegmentHeaders {
    LOG_FILE("%flf"),
    CLOSE_FILE("!ctl"),
    JUMP_LIST("+jmp"),
    CHUNK_START("-st-"),
    FILE_CHUNK("~fc~");
    
    private final byte[] value;
    private final int    validator;
    
    SegmentHeaders(String value) {
        assert(value.length() == 4);
        try {
            this.value = value.getBytes("ASCII");
            this.validator = produceIntValue(this.value);
        } catch ( UnsupportedEncodingException unsupported ) {
            throw new AssertionError(unsupported);
        }
    }
    
    private int produceIntValue(byte[] value) {
        if ( value.length != 4 ) throw new AssertionError("segment headers must be 4 bytes long");
        int val = value[0] & 0xff;
        val = val << Byte.SIZE;
        val |= (value[1] & 0xff);
        val = val << Byte.SIZE;
        val |= (value[2] & 0xff);
        val = val << Byte.SIZE;
        val |= (value[3] & 0xff);
        return val;
    }
    
    public boolean validate(int test) {
        return test == this.validator;
    }
    
    public boolean validate(byte[] test) {
        return Arrays.equals(this.value, test);
    }
    
    public int getIntValue() {
        return validator;
    }
    
    public byte[] getBytes() {
        return this.value;
    }

    @Override
    public String toString() {
        try {
            return new String(this.value,"ASCII");
        } catch ( UnsupportedEncodingException unsupported ) {
            throw new AssertionError(unsupported);
        }
    }
    
    
}
