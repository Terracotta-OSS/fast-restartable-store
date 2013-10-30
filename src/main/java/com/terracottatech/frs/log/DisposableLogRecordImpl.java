/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.terracottatech.frs.log;

import com.terracottatech.frs.Disposable;
import com.terracottatech.frs.io.Chunk;
import java.io.Closeable;
import java.io.IOException;

/**
 *
 * @author mscott
 */
public class DisposableLogRecordImpl extends LogRecordImpl implements Disposable {
    
    private final Closeable resource;

    public DisposableLogRecordImpl(Chunk resource) {
        super(resource.getBuffers(), null);
        if ( resource instanceof Closeable ) {
          this.resource = (Closeable)resource;
        } else {
          this.resource = null;
        }
    }

    @Override
    public void dispose() {
        try {
            if ( resource != null ) {
              resource.close();
            }
        } catch ( IOException ioe ) {
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public void close() throws IOException {
      if ( resource != null ) {
        resource.close();
      }
    }
}
