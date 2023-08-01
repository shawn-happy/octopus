package com.octopus.operators.kettlex.runtime.executor.runner;

import com.octopus.operators.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.operators.kettlex.core.steps.Reader;
import com.octopus.operators.kettlex.core.steps.config.StepConfigChannelCombination;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReaderRunner extends AbstractRunner implements Runnable {

  private final Reader<?> reader;

  public ReaderRunner(Reader<?> reader, StepConfigChannelCombination<?> combination) {
    super(reader, combination);
    this.reader = reader;
  }

  @Override
  public void run() {
    try {
      reader.read();
      markRun();
    } catch (Exception e) {
      log.error("Reader runner Received Exceptions:", e);
      markFail(e);
      throw new KettleXStepExecuteException(e);
    } finally {
      reader.destroy();
    }
  }
}
