package com.octopus.kettlex.runtime.executor.runner;

import com.octopus.kettlex.core.steps.Reader;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReaderRunner extends AbstractRunner implements Runnable {

  private final Reader<?> reader;

  public ReaderRunner(Reader<?> reader) {
    super(reader);
    this.reader = reader;
  }

  @Override
  public void run() {
    try {
      reader.read();
      markRun();
    } catch (Throwable e) {
      log.error("Reader runner Received Exceptions:", e);
      markFailed();
    } finally {
      reader.destroy();
    }
  }
}