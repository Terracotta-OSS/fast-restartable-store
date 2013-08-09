/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.frs;

/**
 *
 * @author mscott
 */
public interface Tuple<I, K, V> {
  
  I getIdentifier();
  
  K getKey();
  
  V getValue();
}
