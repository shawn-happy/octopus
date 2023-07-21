package com.octopus.operators.kettlex.runtime.executor.runner;

import com.octopus.operators.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.operators.kettlex.core.steps.Writer;
import com.octopus.operators.kettlex.core.steps.config.StepConfigChannelCombination;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WriterRunner extends AbstractRunner implements Runnable {

  private final Writer<?> writer;

  public WriterRunner(Writer<?> writer, StepConfigChannelCombination<?> combination) {
    super(writer, combination);
    this.writer = writer;
  }

  @Override
  public void run() {
    try {
      writer.writer();
      markRun();
    } catch (Throwable e) {
      log.error("writer runner do transform error:", e);
      markFail(e);
      throw new KettleXStepExecuteException(e);
    } finally {
      writer.destroy();
    }
  }
}
