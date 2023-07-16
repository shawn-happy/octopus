package com.octopus.spark.operators.registry;

public interface Registry<K, V> {

  void register(K key, V value);

  V get(K key);
}
