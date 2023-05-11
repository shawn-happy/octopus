package com.octopus.kettlex.core.steps;

public class StepMetaContextCombination<SM extends StepMeta, SC extends StepContext> {

  private String stepName;
  private Step<SM, SC> step;
  private SM stepMeta;
  private SC stepContext;

  public StepMetaContextCombination(){}

  public String getStepName() {
    return stepName;
  }

  public void setStepName(String stepName) {
    this.stepName = stepName;
  }

  public Step<SM, SC> getStep() {
    return step;
  }

  public void setStep(Step<SM, SC> step) {
    this.step = step;
  }

  public SM getStepMeta() {
    return stepMeta;
  }

  public void setStepMeta(SM stepMeta) {
    this.stepMeta = stepMeta;
  }

  public SC getStepContext() {
    return stepContext;
  }

  public void setStepContext(SC stepContext) {
    this.stepContext = stepContext;
  }
}
