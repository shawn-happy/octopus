package com.octopus.kettlex.core.steps.writer.log;

import com.octopus.kettlex.core.monitor.ExecutionStatus;
import com.octopus.kettlex.core.steps.StepContext;

public class LogMessageContext implements StepContext {

  @Override
  public void markStatus(ExecutionStatus status) {}
}
