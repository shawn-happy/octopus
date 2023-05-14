package com.octopus.kettlex.core.step;

import com.octopus.kettlex.core.statistics.ExecutionState;
import com.octopus.kettlex.core.steps.Step;
import com.octopus.kettlex.core.steps.StepContext;
import com.octopus.kettlex.core.steps.StepMeta;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StepRunThread implements Runnable {

  private final Step step;
  private final StepMeta meta;
  private final StepContext context;

  public StepRunThread(Step step) {
    this.step = step;
    this.meta = step.getStepMeta();
    this.context = step.getStepContext();
  }

  @Override
  public void run() {
    try {
      step.markStatus(ExecutionState.RUNNING);
      // Wait
      while (step.processRow()) {
        if (step.isStopped()) {
          break;
        }
      }
    } catch (Throwable t) {
      log.error("step execute error", t);
      step.setErrors(1);
      step.markStatus(ExecutionState.FAILED);
      step.stopAll();
    } finally {
      step.dispose();
      step.getStepContext().reportStepCommunication();
    }
  }
}
