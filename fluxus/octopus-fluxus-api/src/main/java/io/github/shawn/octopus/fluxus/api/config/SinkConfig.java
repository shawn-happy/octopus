package io.github.shawn.octopus.fluxus.api.config;

public interface SinkConfig<P extends SinkConfig.SinkOptions> extends StepConfig {

  String getInput();

  P getOptions();

  SinkConfig<P> toSinkConfig(String json);

  @Override
  default PluginType getPluginType() {
    return PluginType.SINK;
  }

  interface SinkOptions {}
}
