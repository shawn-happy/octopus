package com.octopus.operators.engine.config.step;

import com.fasterxml.jackson.databind.JsonNode;
import com.octopus.operators.engine.config.CheckResult;
import com.octopus.operators.engine.util.YamlUtils;
import java.util.List;

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

  void loadYaml(JsonNode stepNode);

  List<CheckResult> check();

  default String toYaml() {
    return YamlUtils.toYaml(this);
  }
}
