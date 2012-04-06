/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.action.ActionCodec;

/**
 * @author tim
 */
public abstract class MapActions {
  public static void registerActions(int id, ActionCodec codec) {
    codec.registerAction(id, 0, PutAction.class);
    codec.registerAction(id, 1, RemoveAction.class);
    codec.registerAction(id, 2, DeleteAction.class);
  }
}
