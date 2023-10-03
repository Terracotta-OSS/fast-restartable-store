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
package com.terracottatech.frs.util;

import java.util.AbstractSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.Stack;

public class AATreeSet<T extends Comparable<? super T>> extends AbstractSet<T> implements SortedSet<T> {

  private Node<T> root = TerminalNode.<T>terminal();

  private int     size = 0;
  private boolean mutated;

  private Node<T> item = TerminalNode.<T>terminal(), heir = TerminalNode.<T>terminal();
  private T       removed;

  @Override
  public boolean add(T o) {
    try {
      root = insert(root, o);
      if (mutated) {
        size++;
      }
      return mutated;
    } finally {
      mutated = false;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean remove(Object o) {
    try {
      root = remove(root, (T) o);
      if (mutated) {
        size--;
      }
      return mutated;
    } finally {
      heir = TerminalNode.<T>terminal();
      item = TerminalNode.<T>terminal();
      mutated = false;
      removed = null;
    }
  }

  @SuppressWarnings("unchecked")
  public T removeAndReturn(Object o) {
    try {
      root = remove(root, (T) o);
      if (mutated) {
        size--;
      }
      return removed;
    } finally {
      heir = TerminalNode.<T>terminal();
      item = TerminalNode.<T>terminal();
      mutated = false;
      removed = null;
    }
  }

  @Override
  public void clear() {
    root = TerminalNode.<T>terminal();
    size = 0;
  }

  @Override
  public Iterator<T> iterator() {
    return new TreeIterator();
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isEmpty() {
    return root == TerminalNode.<T>terminal();
  }

  public Comparator<? super T> comparator() {
    return null;
  }

  public SortedSet<T> subSet(T fromElement, T toElement) {
    return new SubSet(fromElement, toElement);
  }

  public SortedSet<T> headSet(T toElement) {
    return new SubSet(null, toElement);
  }

  public SortedSet<T> tailSet(T fromElement) {
    return new SubSet(fromElement, null);
  }

  public T first() {
    Node<T> leftMost = root;
    while (leftMost.getLeft() != TerminalNode.<T>terminal()) {
      leftMost = leftMost.getLeft();
    }
    return leftMost.getPayload();
  }

  public T last() {
    Node<T> rightMost = root;
    while (rightMost.getRight() != TerminalNode.<T>terminal()) {
      rightMost = rightMost.getRight();
    }
    return rightMost.getPayload();
  }

  @SuppressWarnings("unchecked")
  public T find(Object probe) {
    return find(root, (T) probe).getPayload();
  }

  protected final Node<T> getRoot() {
      return root;
  }

  private Node<T> find(Node<T> top, T probe) {
    if (top == TerminalNode.<T>terminal()) {
      return top;
    } else {
      int direction = top.getPayload().compareTo(probe);
      if (direction > 0) {
        return find(top.getLeft(), probe);
      } else if (direction < 0) {
        return find(top.getRight(), probe);
      } else {
        return top;
      }
    }
  }

  private Node<T> insert(Node<T> top, T data) {
    if (top == TerminalNode.<T>terminal()) {
      mutated = true;
      return createNode(data);
    } else {
      int direction = top.getPayload().compareTo(data);
      if (direction > 0) {
        top.setLeft(insert(top.getLeft(), data));
      } else if (direction < 0) {
        top.setRight(insert(top.getRight(), data));
      } else {
        return top;
      }
      top = skew(top);
      top = split(top);
      return top;
    }
  }

  @SuppressWarnings("unchecked")
  private Node<T> createNode(T data) {
    if (data instanceof Node<?>) {
      return (Node<T>) data;
    } else {
      return new TreeNode<T>(data);
    }
  }

  private Node<T> remove(Node<T> top, T data) {
    if (top != TerminalNode.<T>terminal()) {
      int direction = top.getPayload().compareTo(data);

      heir = top;
      if (direction > 0) {
        top.setLeft(remove(top.getLeft(), data));
      } else {
        item = top;
        top.setRight(remove(top.getRight(), data));
      }

      if (top == heir) {
        if (item != TerminalNode.<T>terminal() && item.getPayload().compareTo(data) == 0) {
          mutated = true;
          item.swapPayload(top);
          removed = top.getPayload();
          top = top.getRight();
        }
      } else {
        if (top.getLeft().getLevel() < top.getLevel() - 1 || top.getRight().getLevel() < top.getLevel() - 1) {
          if (top.getRight().getLevel() > top.decrementLevel()) {
            top.getRight().setLevel(top.getLevel());
          }

          top = skew(top);
          top.setRight(skew(top.getRight()));
          top.getRight().setRight(skew(top.getRight().getRight()));
          top = split(top);
          top.setRight(split(top.getRight()));
        }
      }
    }
    return top;
  }

  private static <T extends Comparable<? super T>> Node<T> skew(Node<T> top) {
    if (top.getLeft().getLevel() == top.getLevel() && top.getLevel() != 0) {
      Node<T> save = top.getLeft();
      top.setLeft(save.getRight());
      save.setRight(top);
      top = save;
    }

    return top;
  }

  private static <T extends Comparable<? super T>> Node<T> split(Node<T> top) {
    if (top.getRight().getRight().getLevel() == top.getLevel() && top.getLevel() != 0) {
      Node<T> save = top.getRight();
      top.setRight(save.getLeft());
      save.setLeft(top);
      top = save;
      top.incrementLevel();
    }

    return top;
  }

  public static interface Node<E extends Comparable<? super E>> {

    public void setLeft(Node<E> node);

    public void setRight(Node<E> node);

    public Node<E> getLeft();

    public Node<E> getRight();

    public int getLevel();

    public void setLevel(int value);

    public int decrementLevel();

    public int incrementLevel();

    public void swapPayload(Node<E> with);

    public E getPayload();
  }

  public static abstract class AbstractTreeNode<E extends Comparable<? super E>> implements Node<E> {

    private Node<E> left;
    private Node<E> right;
    private int     level;

    public AbstractTreeNode() {
      this(1);
    }

    private AbstractTreeNode(int level) {
      this.left = TerminalNode.<E>terminal();
      this.right = TerminalNode.<E>terminal();
      this.level = level;
    }

    public void setLeft(Node<E> node) {
      left = node;
    }

    public void setRight(Node<E> node) {
      right = node;
    }

    public Node<E> getLeft() {
      return left;
    }

    public Node<E> getRight() {
      return right;
    }

    public int getLevel() {
      return level;
    }

    public void setLevel(int value) {
      level = value;
    }

    public int decrementLevel() {
      return --level;
    }

    public int incrementLevel() {
      return ++level;
    }
  }

  private static final class TreeNode<E extends Comparable<? super E>> extends AbstractTreeNode<E> {

    private E payload;

    public TreeNode(E payload) {
      super();
      this.payload = payload;
    }

    public void swapPayload(Node<E> node) {
      if (node instanceof TreeNode<?>) {
        TreeNode<E> treeNode = (TreeNode<E>) node;
        E temp = treeNode.payload;
        treeNode.payload = this.payload;
        this.payload = temp;
      } else {
        throw new IllegalArgumentException();
      }
    }

    public E getPayload() {
      return payload;
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static final class TerminalNode extends AbstractTreeNode {

    private static final Node<?> TERMINAL = new TerminalNode();

    public static <T extends Comparable<? super T>> Node<T> terminal() {
      return (Node<T>) TERMINAL;
    }

    private TerminalNode() {
      super(0);
      super.setLeft(this);
      super.setRight(this);
    }

    @Override
    public void setLeft(Node right) {
        if (right != TERMINAL) {
            throw new AssertionError();
        }
    }

    @Override
    public void setRight(Node left) {
        if (left != TERMINAL) {
            throw new AssertionError();
        }
    }

    @Override
    public void setLevel(int value) {
      throw new AssertionError();
    }

    @Override
    public int decrementLevel() {
      throw new AssertionError();
    }

    @Override
    public int incrementLevel() {
      throw new AssertionError();
    }

    public void swapPayload(Node payload) {
      throw new AssertionError();
    }

    public Comparable getPayload() {
      return null;
    }
  }

  class SubSet extends AbstractSet<T> implements SortedSet<T> {

    private final T start;
    private final T end;

    SubSet(T start, T end) {
      this.start = start;
      this.end = end;
    }

    @Override
    public boolean add(T o) {
      if (inRange(o)) {
        return AATreeSet.this.add(o);
      } else {
        throw new IllegalArgumentException();
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object o) {
      if (inRange((T) o)) {
        return remove(o);
      } else {
        return false;
      }
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<T> iterator() {
      if (end == null) {
        return new SubTreeIterator(start);
      } else {
        throw new UnsupportedOperationException();
      }
    }

    @Override
    public int size() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
      return !iterator().hasNext();
    }

    public Comparator<? super T> comparator() {
      return null;
    }

    public SortedSet<T> subSet(T fromElement, T toElement) {
      if (inRangeInclusive(fromElement) && inRangeInclusive(toElement)) {
        return new SubSet(fromElement, toElement);
      } else {
        throw new IllegalArgumentException();
      }
    }

    public SortedSet<T> headSet(T toElement) {
      if (inRangeInclusive(toElement)) {
        return new SubSet(start, toElement);
      } else {
        throw new IllegalArgumentException();
      }
    }

    public SortedSet<T> tailSet(T fromElement) {
      if (inRangeInclusive(fromElement)) {
        return new SubSet(fromElement, end);
      } else {
        throw new IllegalArgumentException();
      }
    }

    public T first() {
      if (start == null) {
        return AATreeSet.this.first();
      } else {
        throw new UnsupportedOperationException();
      }
    }

    public T last() {
      if (end == null) {
        return AATreeSet.this.last();
      } else {
        throw new UnsupportedOperationException();
      }
    }

    private boolean inRange(T value) {
      return (start == null || start.compareTo(value) <= 0) && (end == null || end.compareTo(value) > 0);
    }

    private boolean inRangeInclusive(T value) {
      return (start == null || start.compareTo(value) <= 0) && (end == null || end.compareTo(value) >= 0);
    }
  }

  class TreeIterator implements Iterator<T> {

    private final Stack<Node<T>> path = new Stack<Node<T>>();
    private Node<T>                        next;

    TreeIterator() {
      path.push(TerminalNode.<T>terminal());
      Node<T> leftMost = root;
      while (leftMost.getLeft() != TerminalNode.<T>terminal()) {
        path.push(leftMost);
        leftMost = leftMost.getLeft();
      }
      next = leftMost;
    }

    TreeIterator(T start) {
      path.push(TerminalNode.<T>terminal());
      Node<T> current = root;
      while (true) {
        int direction = current.getPayload().compareTo(start);
        if (direction > 0) {
          if (current.getLeft() == TerminalNode.<T>terminal()) {
            next = current;
            break;
          } else {
            path.push(current);
            current = current.getLeft();
          }
        } else if (direction < 0) {
          if (current.getRight() == TerminalNode.<T>terminal()) {
            next = path.pop();
            break;
          } else {
            current = current.getRight();
          }
        } else {
          next = current;
          break;
        }
      }
    }

    public boolean hasNext() {
      return next != TerminalNode.<T>terminal();
    }

    public T next() {
      Node<T> current = next;
      advance();
      return current.getPayload();
    }

    private void advance() {
      Node<T> successor = next.getRight();
      if (successor != TerminalNode.<T>terminal()) {
        while (successor.getLeft() != TerminalNode.<T>terminal()) {
          path.push(successor);
          successor = successor.getLeft();
        }
        next = successor;
      } else {
        next = path.pop();
      }
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

  class SubTreeIterator extends TreeIterator {
    public SubTreeIterator(T start) {
      super(start);
    }
  }
}
