package com.octopus.operators.engine.config.step;

import com.octopus.operators.engine.config.CheckResult;
import com.octopus.operators.engine.util.YamlUtils;
import java.util.List;

public interface StepOptions {

  StepOptions loadYaml(String yaml);

  List<CheckResult> check();

  default String toYaml() {
    return YamlUtils.toYaml(this);
  }
}
