package com.octopus.kettlex.runtime.executor.runner;

import com.octopus.kettlex.core.steps.Writer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WriterRunner extends AbstractRunner implements Runnable {

  private final Writer<?> writer;

  public WriterRunner(Writer<?> writer) {
    super(writer);
    this.writer = writer;
  }

  @Override
  public void run() {
    try {
      writer.writer();
      markRun();
    } catch (Throwable e) {
      log.error("writer runner do transform error:", e);
      markFailed();
    } finally {
      writer.destroy();
    }
  }
}
