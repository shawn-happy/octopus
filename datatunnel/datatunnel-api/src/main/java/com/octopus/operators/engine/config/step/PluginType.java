package com.octopus.operators.engine.config.step;

import com.octopus.operators.engine.exception.EngineException;
import java.util.Arrays;
import lombok.Getter;

@Getter
public enum PluginType {
  SOURCE("source"),
  TRANSFORM("transform"),
  SINK("sink"),
  ;

  private final String name;

  PluginType(String name) {
    this.name = name;
  }

  public static PluginType of(String type) {
    return Arrays.stream(values())
        .filter(
            pluginType ->
                pluginType.name().equalsIgnoreCase(type)
                    || pluginType.getName().equalsIgnoreCase(type))
        .findFirst()
        .orElseThrow(() -> new EngineException("No Such PluginType: " + type));
  }
}
