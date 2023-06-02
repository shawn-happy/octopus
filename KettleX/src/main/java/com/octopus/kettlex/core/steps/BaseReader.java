package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.model.ReaderConfig;

public abstract class BaseReader<C extends ReaderConfig<?>> extends BaseStep<C> {

  protected BaseReader(C stepConfig) {
    super(stepConfig);
  }
}
