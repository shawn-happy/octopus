package com.octopus.operators.engine.config.step;

import com.octopus.operators.engine.util.YamlUtils;

/**
 * Step Config
 *
 * @param <OPTIONS>
 */
public interface StepConfig<OPTIONS extends StepOptions> {
  PluginType getPluginType();

  String getId();

  String getName();

  String getType();

  String getDescription();

  OPTIONS getOptions();

  Integer getParallelism();

  StepConfig<OPTIONS> loadYaml(String yaml);

  default String toYaml() {
    return YamlUtils.toYaml(this);
  }
}
