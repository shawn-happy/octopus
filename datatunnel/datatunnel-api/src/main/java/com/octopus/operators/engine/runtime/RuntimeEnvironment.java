package com.octopus.operators.engine.runtime;

import com.octopus.operators.engine.config.job.JobConfig;
import com.octopus.operators.engine.config.job.JobMode;

public interface RuntimeEnvironment<T extends JobConfig> {
  T getJobConfig();

  void setJobConfig(T jobConfig);

  default JobMode getJobMode() {
    return getJobConfig().getMode();
  }

  void prepare();
}
