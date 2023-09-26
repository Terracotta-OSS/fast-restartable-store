/*
 * Copyright (c) 2012-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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
package com.terracottatech.frs.object.heap;

import com.terracottatech.frs.object.ValueSortedMap;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

/**
 *
 * @author cdennis
 */
public class HeapValueSortedMap<K, V extends Comparable<V>> implements ValueSortedMap<K, V> {

  private final Map<K, Node<K, V>> map = new HashMap<K, Node<K, V>>();
  private final PriorityQueue<Node<K, V>> sorted = new PriorityQueue<Node<K, V>>();
  
  @Override
  public K firstKey() {
    Node<K, V> first = sorted.peek();
    if (first == null) {
      return null;
    } else {
      return first.key;
    }
  }

  @Override
  public V firstValue() {
    Node<K, V> first = sorted.peek();
    if (first == null) {
      return null;
    } else {
      return first.value;
    }
  }

  @Override
  public void put(K key, V value) {
    Node<K, V> node = new Node<K, V>(key, value);
    sorted.remove(map.put(key, node));
    sorted.add(node);
  }

  @Override
  public void remove(K key) {
    sorted.remove(map.remove(key));
  }

  public void clear() {
    sorted.clear();
    map.clear();
  }
  
  @Override
  public V get(K key) {
    Node<K, V> node = map.get(key);
    if (node == null) {
      return null;
    } else {
      return node.value;
    }
  }

  @Override
  public int size() {
    return map.size();
  }
  
  static class Node<K, V extends Comparable<V>> implements Comparable<Node<K, V>> {
    
    private final K key;
    private final V value;
    
    Node(K key, V value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public int compareTo(Node<K, V> t) {
      return value.compareTo(t.value);
    }
    
  }

}
