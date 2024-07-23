/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.Chunk;
import java.nio.ByteBuffer;
import java.util.Collections;

/**
 *
 * @author mscott
 */
public class CopyingPacker extends LogRegionPacker {
    
    private final BufferSource pool;
    private static final int FUTURE_SPACER = 64;
    
    public CopyingPacker(Signature sig, String forceLogRegionFormat, BufferSource copyInto) {
        super(sig, forceLogRegionFormat);
        pool = copyInto;
    }   

    @Override
    protected Chunk writeRecords(Iterable<LogRecord> records) {     
      Chunk base = super.writeRecords(records);
      ByteBuffer grp = pool.getBuffer((int)base.length() + FUTURE_SPACER * 2);
      grp.position(FUTURE_SPACER);
      for ( ByteBuffer b : base.getBuffers() ) { 
        grp.put(b);
      }
      grp.flip();
      grp.position(FUTURE_SPACER);
      return new BufferListWrapper(Collections.singletonList(grp), pool);
    }
}
