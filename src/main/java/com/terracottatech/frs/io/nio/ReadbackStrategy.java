/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import java.io.IOException;

/**
 *
 * @author mscott
 */
public interface ReadbackStrategy extends Iterable<Chunk> {
    Chunk iterate(Direction dir) throws IOException;
    boolean hasMore(Direction dir) throws IOException;
    void queue(Direction dir) throws IOException;
}
