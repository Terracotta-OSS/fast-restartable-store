package com.terracottatech.frs.object;

/**
 * Interface to be implemented by some general restartable object.
 *
 * @author tim
 */
public interface RestartableObject<I, K, V> {

  /**
   * Get the identitfier for this object
   *
   * @return identifier
   */
  public I getId();

  /**
   * Get this object's ObjectManagerStripe
   *
   * @return the object's ObjectManagerStripe
   */
  public ObjectManagerStripe<I, K, V> getObjectManagerStripe();
}
