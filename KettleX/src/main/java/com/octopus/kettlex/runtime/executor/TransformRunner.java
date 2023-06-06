package com.octopus.kettlex.runtime.executor;

import com.octopus.kettlex.core.steps.Transform;
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
    } catch (Throwable e) {
      log.error("transformation runner do transform error:", e);
    } finally {
      transform.destroy();
    }
  }
}
