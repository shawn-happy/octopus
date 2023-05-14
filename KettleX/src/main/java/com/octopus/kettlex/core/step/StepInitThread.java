package com.octopus.kettlex.core.step;

import com.octopus.kettlex.core.exception.KettleXTransException;
import com.octopus.kettlex.core.statistics.ExecutionState;
import com.octopus.kettlex.core.steps.Step;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StepInitThread implements Runnable {

  private boolean ok;
  private boolean finished;
  private boolean doIt;
  private Step step;

  public StepInitThread(Step step) {
    this.step = step;
    this.ok = false;
    this.finished = false;
    this.doIt = true;
  }

  @Override
  public void run() {
    if (!doIt) {
      return;
    }
    try {
      if (this.step.init()) {
        this.step.markStatus(ExecutionState.INITIALIZING);
        ok = true;
      } else {
        step.setErrors(1);
        this.step.markStatus(ExecutionState.FAILED);
        ok = false;
      }
    } catch (Throwable e) {

      throw new KettleXTransException("Error initializing step");
    }

    finished = true;
  }

  public boolean isFinished() {
    return finished;
  }

  public boolean isOk() {
    return ok;
  }

  /** @return the doIt */
  public boolean isDoIt() {
    return doIt;
  }

  /** @param doIt the doIt to set */
  public void setDoIt(boolean doIt) {
    this.doIt = doIt;
  }

  public Step getStep() {
    return step;
  }
}
