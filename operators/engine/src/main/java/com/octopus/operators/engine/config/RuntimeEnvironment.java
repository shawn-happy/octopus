package com.octopus.operators.engine.config;

import java.util.Optional;

public interface RuntimeEnvironment {

  EngineType getEngine();

  void setTaskConfig(TaskConfig taskConfig);

  TaskConfig getTaskConfig();

  CheckResult checkTaskConfig();

  RuntimeEnvironment prepare();

  default TaskMode getTaskMode() {
    return Optional.ofNullable(getTaskConfig()).orElse(TaskConfig.builder().build()).getTaskMode();
  }
}
