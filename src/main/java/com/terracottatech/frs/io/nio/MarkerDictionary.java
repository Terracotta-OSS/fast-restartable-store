/*
 * Copyright (c) 2017-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.util.LongLongOrderedDeltaArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.terracottatech.frs.util.LongLongOrderedDeltaArray.LongLongEntry;

/**
 * @author cschanck
 **/
public class MarkerDictionary {

  private static Logger LOG = LoggerFactory.getLogger(MarkerDictionary.class);

  private LongLongOrderedDeltaArray map = new LongLongOrderedDeltaArray(512);
  private int count = 0;

  public void append(long key, long val) {
    if (map.isEmpty() || key > map.getKey(map.size() - 1)) {
      map.append(key, val);
      count++;
    } else {
      throw new UnsupportedOperationException(key + " vs " + map.getKey(map.size() - 1));
    }
  }

  public void clear() {
    count = 0;
    map.clear();
  }

  // >= key or null if there is none.
  public LongLongEntry ceilingEntry(long from) {
    int pos = map.binarySearch(from);
    if (pos < 0) {
      pos = ~pos;
    }
    if (pos >= 0 && pos < map.size()) {
      LongLongEntry ent = map.get(pos);
      return ent;
    }
    return null;
  }

  //
  public LongLongEntry floorEntry(long from) {
    int pos = map.binarySearch(from);
    if (pos < 0) {
      pos = ~pos;
      pos--;
    }
    if (pos >= 0 && pos < map.size()) {
      LongLongEntry ent = map.get(pos);
      return ent;
    }
    return null;
  }

  public boolean isEmpty() {
    return count == 0;
  }

  public int size() {
    return count;
  }

  public LongLongEntry lastEntry() {
    if (map.isEmpty()) {
      return null;
    }
    LongLongEntry ent = map.get(map.size() - 1);
    return ent;
  }

  public LongLongEntry firstEntry() {
    if (map.isEmpty()) {
      return null;
    }
    LongLongEntry ent = map.get(0);
    return ent;
  }

  public boolean replace(long key, long value) {
    int pos = map.binarySearch(key);
    if (pos >= 0) {
      map.update(pos, key, value);
      return true;
    }
    return false;
  }

  public LongLongEntry get(long key) {
    int pos = map.binarySearch(key);
    if (pos >= 0) {
      return map.get(pos);
    }
    return null;
  }

}
