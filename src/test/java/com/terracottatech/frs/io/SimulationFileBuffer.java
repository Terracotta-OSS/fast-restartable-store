/*
 * Copyright (c) 2012-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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
package com.terracottatech.frs.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 *
 * @author mscott
 */
public class SimulationFileBuffer extends FileBuffer {
    
    BufferFilter filters;

    public SimulationFileBuffer(FileChannel channel, ByteBuffer src) throws IOException {
        super(channel, src);
        filters = GlobalFilters.getFilters();
    }

    @Override
    public long writeFully(ByteBuffer buffer) throws IOException {
        BufferFilter f = filters;
        while ( f != null ) {
            if ( !f.filter(buffer) ) {
                return 0;
            }
            f = f.next();
        }
        return super.writeFully(buffer);
    }
    
    public void addFilter(BufferFilter filter) {
        if ( filters == null ) filters = filter;
        else filters.add(filter);
    }
    
    public void setFilters(BufferFilter filter) {
        filters = filter;
    }

}
