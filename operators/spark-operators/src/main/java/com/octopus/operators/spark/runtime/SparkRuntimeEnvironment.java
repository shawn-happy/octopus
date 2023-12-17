package com.octopus.operators.spark.runtime;

import com.octopus.operators.engine.config.CheckResult;
import com.octopus.operators.engine.config.EngineType;
import com.octopus.operators.engine.config.RuntimeEnvironment;
import com.octopus.operators.engine.config.TaskConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SparkRuntimeEnvironment implements RuntimeEnvironment {

  @Default private EngineType engine = EngineType.SPARK;
  @Setter private TaskConfig taskConfig;

  @Override
  public CheckResult checkTaskConfig() {
    return null;
  }
}
