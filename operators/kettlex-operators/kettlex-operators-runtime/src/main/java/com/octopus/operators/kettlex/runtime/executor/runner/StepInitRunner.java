package com.octopus.operators.kettlex.runtime.executor.runner;

import com.octopus.operators.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.operators.kettlex.core.management.ExecutionStatus;
import com.octopus.operators.kettlex.core.steps.Step;
import com.octopus.operators.kettlex.core.steps.config.StepConfig;
import com.octopus.operators.kettlex.core.steps.config.StepConfigChannelCombination;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class StepInitRunner<C extends StepConfig<?>> extends AbstractRunner implements Runnable {

  public boolean ok;
  public boolean finished;

  private final Step<C> step;
  private final StepConfigChannelCombination<C> combination;

  public StepInitRunner(Step<C> step, StepConfigChannelCombination<C> combination) {
    super(step, combination);
    this.step = step;
    this.combination = combination;
    this.ok = false;
    this.finished = false;
  }

  @Override
  public void run() {
    try {
      if (step.init(combination)) {
        ok = true;
      } else {
        ok = false;
        mark(ExecutionStatus.FAILED);
        log.error("Error initializing step {}.", step.getStepConfig().getName());
      }
    } catch (Throwable e) {
      markFail(e);
      log.error("Error initializing step {}.", step.getStepConfig().getName(), e);
      throw new KettleXStepExecuteException(
          String.format("Error initializing step %s.", step.getStepConfig().getName()), e);
    }
    finished = true;
  }
}
