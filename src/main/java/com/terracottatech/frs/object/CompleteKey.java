/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object;

/**
 *
 * @author cdennis
 */
public interface CompleteKey<I, K> {
  I getId();
  K getKey();
}
