package com.octopus.kettlex.core.steps;

public abstract class BaseStep implements Step {

  private StepMeta stepMeta;
  private StepContext stepContext;

  public BaseStep(StepMeta stepMeta) {
    this.stepMeta = stepMeta;
    this.stepContext = stepMeta.getStepContext();
  }

  public BaseStep(StepMeta stepMeta, StepContext stepContext) {
    this.stepMeta = stepMeta;
    this.stepContext = stepContext;
  }
}
