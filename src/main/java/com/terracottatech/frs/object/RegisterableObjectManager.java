package com.terracottatech.frs.object;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Aggregated ObjectManager that supports registering stripes.
 *
 * @author tim
 */
public class RegisterableObjectManager<I, K, V> extends AbstractObjectManager<I, K, V> {

  private final ConcurrentMap<I, ObjectManagerStripe<I, K, V>> stripes = new ConcurrentHashMap<I, ObjectManagerStripe<I, K, V>>();

  @Override
  protected ObjectManagerStripe<I, K, V> getStripeFor(I id) {
    return stripes.get(id);
  }

  @Override
  public int replayConcurrency(I id, K key) {
    ObjectManagerStripe<I, K, V> stripe = stripes.get(id);
    int concurrency = stripe.replayConcurrency(key);
    return (concurrency == 1) ? stripe.hashCode() : concurrency;
  }

  @Override
  protected Collection<ObjectManagerStripe<I, K, V>> getStripes() {
    return stripes.values();
  }

  public void registerObject(RestartableObject<I, K, V> object) {
    registerStripe(object.getId(), object.getObjectManagerStripe());
  }

  public void registerStripe(I id, ObjectManagerStripe<I, K, V> stripe) {
    ObjectManagerStripe<?, ?, ?> previous = stripes.putIfAbsent(id, stripe);
    if (previous != null) {
      throw new AssertionError(id + " already mapped");
    }
  }
  
  public void unregisterStripe(I id) {
    if (stripes.remove(id) == null) {
      throw new AssertionError(id + " not mapped");
    } 
  }
}
