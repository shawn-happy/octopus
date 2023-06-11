package com.octopus.kettlex.core.steps.config;

import com.octopus.kettlex.core.exception.KettleXStepConfigException;
import com.octopus.kettlex.core.steps.config.ReaderConfig.ReaderOptions;
import org.apache.commons.lang3.StringUtils;

public interface ReaderConfig<P extends ReaderOptions> extends StepConfig<P> {

  String getOutput();

  @Override
  default void verify() {
    StepConfig.super.verify();
    if (StringUtils.isBlank(getOutput())) {
      throw new KettleXStepConfigException(
          String.format("output cannot be null in reader %s.", getName()));
    }
  }

  interface ReaderOptions extends StepOptions {}
}
