package com.octopus.kettlex.core.plugin;

import com.octopus.kettlex.core.exception.KettleXPluginException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class PluginRegistry {

  private static final PluginRegistry REGISTRY = new PluginRegistry();
  private static final Map<PluginType, Set<PluginInfo>> PLUGINS = new HashMap<>(256);
  private static final Map<String, ClassLoader> CLASS_LOADER_MAP = new HashMap<>(256);
  private static final Map<PluginType, Map<PluginInfo, ClassLoader>> PLUGIN_TYPE_CLASSLOADERS_MAP =
      new HashMap<>(256);
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static PluginRegistry getRegistry() {
    return REGISTRY;
  }

  public void registerPlugin(PluginInfo pluginInfo) {
    PluginType pluginType = pluginInfo.getPluginType();
    String name = pluginInfo.getName();
    lock.writeLock().lock();
    try {
      lock.readLock().lock();
      Set<PluginInfo> pluginInfos = PLUGINS.get(name);
      if (CollectionUtils.isEmpty(pluginInfos)) {
        pluginInfos = new HashSet<>(256);
      }
      lock.readLock().unlock();
      pluginInfos.add(pluginInfo);
      PLUGINS.put(pluginType, pluginInfos);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public PluginInfo getPlugin(PluginType pluginType, String id) {
    if (StringUtils.isBlank(id)) {
      return null;
    }

    // getPlugins() never returns null, see his method above
    return getPlugins(pluginType).stream()
        .filter(plugin -> id.equalsIgnoreCase(plugin.getId()))
        .findFirst()
        .orElse(null);
  }

  public List<PluginInfo> getPlugins(PluginType pluginType) {
    List<PluginInfo> result;
    lock.readLock().lock();
    try {
      result =
          PLUGINS.keySet().stream()
              .filter(py -> pluginType.equals(py))
              .flatMap(py -> PLUGINS.get(py).stream())
              .collect(Collectors.toUnmodifiableList());
    } finally {
      lock.readLock().unlock();
    }
    return result;
  }

  public <T> T loadClass(PluginInfo plugin) {
    return (T) loadClass(plugin, plugin.getMainType());
  }

  public <T> T loadClass(PluginInfo plugin, Class<T> tclass) {
    if (plugin == null) {
      throw new KettleXPluginException("Not a valid plugin");
    }

    String className = plugin.getClassMap().get(tclass);
    if (className == null) {
      throw new KettleXPluginException(
          String.format(
              "Plugin %s is unable to load class %s", plugin.getName(), tclass.getName()));
    }
    try {
      Class<T> cl;
      if (plugin.isNativePlugin()) {
        cl = (Class<T>) Class.forName(className);
      } else {
        ClassLoader ucl = getClassLoader(plugin);
        // Load the class.
        cl = (Class<T>) ucl.loadClass(className);
      }

      return cl.newInstance();
    } catch (Throwable e) {
      throw new KettleXPluginException("Unexpected error loading class", e);
    }
  }

  public ClassLoader getClassLoader(PluginInfo plugin) {
    if (plugin == null) {
      throw new KettleXPluginException("Not a valid plugin");
    }

    try {
      if (plugin.isNativePlugin()) {
        return this.getClass().getClassLoader();
      } else {
        ClassLoader cl;

        lock.writeLock().lock();
        try {
          Map<PluginInfo, ClassLoader> pluginInfoClassLoaderMap =
              PLUGIN_TYPE_CLASSLOADERS_MAP.computeIfAbsent(
                  plugin.getPluginType(), k -> new HashMap<>(256));
          cl = pluginInfoClassLoaderMap.get(plugin);
          if (cl == null) {
            if (plugin.isNativePlugin()) {
              cl = Thread.currentThread().getContextClassLoader();
            } else {
              List<String> libraries = plugin.getLibraries();
              if (CollectionUtils.isEmpty(libraries)) {
                throw new KettleXPluginException("can't load jar if libraries is empty");
              }
              cl = new KettleXURLClassLoader(plugin.getLibraries().toArray(new String[0]));
            }
          }

        } finally {
          lock.writeLock().unlock();
        }

        // Load the class.
        return cl;
      }
    } catch (Throwable e) {
      throw new KettleXPluginException("Error creating class loader", e);
    }
  }
}
