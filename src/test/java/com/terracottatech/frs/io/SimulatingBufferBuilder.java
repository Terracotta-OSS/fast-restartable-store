/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 *
 * @author mscott
 */
public class SimulatingBufferBuilder implements BufferBuilder {

    public SimulatingBufferBuilder() {
    }

    @Override
    public FileBuffer createBuffer(FileChannel channel, BufferSource buffer, int size) throws IOException {
        return new SimulationFileBuffer(channel,buffer.getBuffer(size));
    }
    
}
