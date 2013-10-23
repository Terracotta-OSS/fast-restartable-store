/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.terracottatech.frs.log;

import com.terracottatech.frs.Disposable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * @author mscott
 */
public class DisposableLogRecordImpl extends LogRecordImpl implements Disposable {
    
    private final Closeable resource;

    public DisposableLogRecordImpl(Closeable resource, ByteBuffer[] buffers) {
        super(buffers, null);
        this.resource = resource;
    }

    @Override
    public void dispose() {
        try {
            resource.close();
        } catch ( IOException ioe ) {
            throw new RuntimeException(ioe);
        }
    }
    
}
