/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
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
