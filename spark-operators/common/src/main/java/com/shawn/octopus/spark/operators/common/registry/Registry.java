package com.shawn.octopus.spark.operators.common.registry;

public interface Registry<K, V> {

  void register(K key, V value);

  V get(K key);
}
