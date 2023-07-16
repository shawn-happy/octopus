package com.octopus.operators.kettlex.runtime.executor.runner;

import com.octopus.operators.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.operators.kettlex.core.steps.Transform;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransformRunner extends AbstractRunner implements Runnable {

  private final Transform<?> transform;

  public TransformRunner(Transform<?> transform) {
    super(transform);
    this.transform = transform;
  }

  @Override
  public void run() {
    try {
      transform.processRow();
      markRun();
    } catch (Throwable e) {
      log.error("transformation runner do transform error:", e);
      markFail(e);
      throw new KettleXStepExecuteException(e);
    } finally {
      transform.destroy();
    }
  }
}
