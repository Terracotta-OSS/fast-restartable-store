/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.frs;

import java.io.Closeable;

/**
 *
 * @author mscott
 */
public interface DisposableLifecycle extends Disposable {
  void setDisposable(Closeable c);
}
