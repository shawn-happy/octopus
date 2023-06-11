package com.octopus.kettlex.runtime.executor.runner;

import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.steps.Step;
import com.octopus.kettlex.core.steps.config.StepConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class StepInitRunner<C extends StepConfig<?>> extends AbstractRunner implements Runnable {

  public boolean ok;
  public boolean finished;

  private final Step<C> step;
  private final C stepConfig;

  public StepInitRunner(Step<C> step, C stepConfig) {
    super(step);
    this.step = step;
    this.stepConfig = stepConfig;
    this.ok = false;
    this.finished = false;
  }

  @Override
  public void run() {
    try {
      if (step.init(stepConfig)) {
        ok = true;
      } else {
        ok = false;
        log.error("Error initializing step {}.", step.getStepConfig().getName());
      }
    } catch (Throwable e) {
      log.error("Error initializing step {}.", step.getStepConfig().getName(), e);
      throw new KettleXStepExecuteException(
          String.format("Error initializing step %s.", step.getStepConfig().getName()), e);
    }
    finished = true;
  }
}
