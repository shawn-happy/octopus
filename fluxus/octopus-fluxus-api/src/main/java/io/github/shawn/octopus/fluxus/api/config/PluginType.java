package io.github.shawn.octopus.fluxus.api.config;

import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import java.util.Arrays;
import lombok.Getter;

@Getter
public enum PluginType {
  SOURCE("source"),
  TRANSFORM("transform"),
  SINK("sink");

  private final String type;

  PluginType(String type) {
    this.type = type;
  }

  public static PluginType of(String pluginType) {
    return Arrays.stream(values())
        .filter(e -> e.type.equalsIgnoreCase(pluginType))
        .findAny()
        .orElseThrow(
            () -> new DataWorkflowException(String.format("no such plugin type: %s", pluginType)));
  }
}
