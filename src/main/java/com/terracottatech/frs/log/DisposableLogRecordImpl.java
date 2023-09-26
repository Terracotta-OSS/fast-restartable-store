/*
 * Copyright (c) 2013-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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

import com.terracottatech.frs.Disposable;
import com.terracottatech.frs.io.Chunk;
import java.io.Closeable;
import java.io.IOException;

/**
 *
 * @author mscott
 */
public class DisposableLogRecordImpl extends LogRecordImpl {
    
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
    public void close() throws IOException {
      super.close();
      if ( resource != null ) {
        resource.close();
      }
    }
}
