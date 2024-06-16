package com.octopus.operators.engine.config.job;

import com.octopus.operators.engine.config.CheckResult;
import com.octopus.operators.engine.config.step.SinkConfig;
import com.octopus.operators.engine.config.step.SourceConfig;
import com.octopus.operators.engine.config.step.StepOptions;
import com.octopus.operators.engine.config.step.TransformConfig;
import com.octopus.operators.engine.util.YamlUtils;
import java.util.List;
import java.util.Map;

public interface JobConfig {

  String getId();

  String getName();

  JobEngine getEngine();

  JobMode getMode();

  List<String> getJars();

  void setJars(List<String> jars);

  List<SourceConfig<? extends StepOptions>> getSources();

  void setSources(List<SourceConfig<? extends StepOptions>> sources);

  List<TransformConfig<? extends StepOptions>> getTransforms();

  void setTransforms(List<TransformConfig<? extends StepOptions>> transforms);

  SinkConfig<? extends StepOptions> getSink();

  void setSink(SinkConfig<? extends StepOptions> sink);

  Integer getParallelism();

  void setParallelism(Integer parallelism);

  Map<String, Object> getExtraConf();

  void setExtraConf(Map<String, Object> extraConf);

  JobConfig loadYaml(String yaml);

  default String toYaml() {
    return YamlUtils.toYaml(this);
  }

  CheckResult checkConfig();
}
