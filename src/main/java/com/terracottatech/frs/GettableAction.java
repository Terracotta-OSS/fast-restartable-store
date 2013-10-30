/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.action.InvalidatingAction;
import java.nio.ByteBuffer;

/**
 *
 * @author mscott
 */
public interface GettableAction extends Tuple<ByteBuffer,ByteBuffer,ByteBuffer>, InvalidatingAction {
    long getLsn();
}
