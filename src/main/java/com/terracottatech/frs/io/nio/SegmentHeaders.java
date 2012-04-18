/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.util.ByteBufferUtils;
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
    
    byte[] value;
    
    SegmentHeaders(String value) {
        assert(value.length() == 4);
        try {
            this.value = value.getBytes("ASCII");
        } catch ( UnsupportedEncodingException unsupported ) {
            this.value = new byte[value.length()];
            for (int x=0;x<value.length();x++) {
                this.value[x] = (byte)value.charAt(x);
            }
        }
    }
    
    public boolean validate(int test) {
        return test == this.getIntValue();
    }
    
    public boolean validate(byte[] test) {
        return Arrays.equals(this.value, test);
    }
    
    public int getIntValue() {
        int val = 0;
        for (int x=0;x<ByteBufferUtils.INT_SIZE;x++) {
            val |= (this.value[x] << ((ByteBufferUtils.INT_SIZE - x - 1) * Byte.SIZE));
        }
        return val;
    }
    
    public static void main(String[] arg) {
        System.out.println(SegmentHeaders.CHUNK_START.getIntValue());
        System.out.println((((byte)'-')<<24)|(((byte)'s')<<16)|(((byte)'t')<<8)|(((byte)'-')));
    }
    
    public byte[] getBytes() {
        return this.value;
    }

    @Override
    public String toString() {
        try {
            return new String(this.value,"ASCII");
        } catch ( UnsupportedEncodingException unsupported ) {
            return new String(this.value);
        }
    }
    
    
}
