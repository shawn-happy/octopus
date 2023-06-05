package com.octopus.kettlex.runtime.executor;

import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.steps.Reader;
import com.octopus.kettlex.core.steps.Step;
import com.octopus.kettlex.core.steps.Transform;
import com.octopus.kettlex.core.steps.Writer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StepExecuteRunner implements Runnable {

  private final Step<?> step;

  public StepExecuteRunner(Step<?> step) {
    this.step = step;
  }

  @Override
  public void run() {
    try {
      if (step instanceof Reader<?>) {
        Reader<?> reader = (Reader<?>) step;
        reader.read();
      } else if (step instanceof Transform<?>) {
        Transform<?> transform = (Transform<?>) step;
        transform.processRow();
      } else if (step instanceof Writer<?>) {
        Writer<?> writer = (Writer<?>) step;
        writer.writer();
      }
    } catch (Throwable e) {
      log.error("step {} run error.", step.getStepConfig().getName(), e);
      throw new KettleXStepExecuteException(
          String.format("step {} run error.", step.getStepConfig().getName()), e);
    }
  }
}
