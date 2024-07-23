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
package com.terracottatech.frs.io;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;

/**
 * Logical continuous Log Stream.
 * @author mscott
 */
public interface Stream extends Iterable<Chunk>,Closeable  {
    
    /* stream back segments in forward or reverse direction  */
    void seek(long loc) throws IOException;
    
    Chunk read(Direction dir) throws IOException;
            
//    long write(Chunk c) throws IOException;
    
    /* close previous segment if any, provide a new segment for appending  */

    long append(Chunk c, long marker) throws IOException;

    UUID getStreamId();
    
    long sync() throws IOException;

}
