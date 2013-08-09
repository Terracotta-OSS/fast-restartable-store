/*
 * All content copyright (c) 2013 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.IOException;

/**
 *
 * @author mscott
 */
public interface RandomAccess {
    Chunk scan(long marker) throws IOException;
}
