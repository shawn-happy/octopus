package io.github.shawn.octopus.fluxus.api.config;

import java.util.List;

public interface TransformConfig<P extends TransformConfig.TransformOptions> extends StepConfig {

  List<String> getInputs();

  String getOutput();

  P getOptions();

  TransformConfig<P> toTransformConfig(String json);

  @Override
  default PluginType getPluginType() {
    return PluginType.TRANSFORM;
  }

  interface TransformOptions {}
}
