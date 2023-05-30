package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.model.Options;
import com.octopus.kettlex.model.StepConfig;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BaseStep<C extends StepConfig<?>> implements Step<C> {

  protected final C stepConfig;

  protected BaseStep(C stepConfig) {
    this.stepConfig = stepConfig;
  }

  @Override
  public boolean init() throws KettleXException {
    Options options = stepConfig.getOptions();
    options.verify();
    log.info("init step config. {}", stepConfig.getName());
    doInit();
    return true;
  }

  @Override
  public void destory() throws KettleXException {
    log.info("destory step. {}", stepConfig.getName());
    doDestory();
  }

  protected void doDestory() throws KettleXException {}

  protected void doInit() throws KettleXException {}
}
