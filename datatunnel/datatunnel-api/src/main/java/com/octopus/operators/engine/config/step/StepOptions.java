package com.octopus.operators.engine.config.step;

import com.octopus.operators.engine.util.YamlUtils;

public interface StepOptions {

  StepOptions loadYaml(String yaml);

  default String toYaml() {
    return YamlUtils.toYaml(this);
  }
}
