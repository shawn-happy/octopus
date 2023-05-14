package com.octopus.kettlex.core.plugin;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class PluginInfo {
  private String id;
  private String name;
  private String description;
  private PluginType pluginType;
  private boolean nativePlugin;
  private Map<Class<?>, String> classMap;
  private Class<?> mainType; // 插件实现类
  private List<String> libraries;
}
