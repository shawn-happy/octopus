package com.octopus.kettlex.core.steps.config;

import com.octopus.kettlex.core.exception.KettleXStepConfigException;
import com.octopus.kettlex.core.steps.config.WriterConfig.WriterOptions;
import org.apache.commons.lang3.StringUtils;

public interface WriterConfig<P extends WriterOptions> extends StepConfig<P> {

  String getInput();

  WriteMode getWriteMode();

  @Override
  default void verify() {
    StepConfig.super.verify();
    if (StringUtils.isBlank(getInput())) {
      throw new KettleXStepConfigException(
          String.format("input cannot be null in writer %s.", getName()));
    }
  }

  interface WriterOptions extends StepOptions {}
}
