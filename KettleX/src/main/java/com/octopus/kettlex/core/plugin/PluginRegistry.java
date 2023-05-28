package com.octopus.kettlex.core.plugin;

public interface PluginRegistry<K, V> {

  public void registePlugin(K key, V value);

  public V get(K key);
}
