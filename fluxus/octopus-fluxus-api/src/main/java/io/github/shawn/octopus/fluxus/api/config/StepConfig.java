package io.github.shawn.octopus.fluxus.api.config;

public interface StepConfig {
  String getIdentifier();

  String getId();

  String getName();

  PluginType getPluginType();

  default boolean isSource() {
    return this instanceof SourceConfig;
  }

  default boolean isTransform() {
    return this instanceof TransformConfig;
  }

  default boolean isSink() {
    return this instanceof SinkConfig;
  }
}
