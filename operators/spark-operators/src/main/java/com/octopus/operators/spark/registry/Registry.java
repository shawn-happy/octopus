package com.octopus.operators.spark.registry;

public interface Registry<K, V> {

  void register(K key, V value);

  V get(K key);
}
