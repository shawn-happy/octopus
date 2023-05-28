package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.monitor.ExecutionStatus;

/**
 * step context
 *
 * @author shawn
 */
public interface StepContext {

  void markStatus(ExecutionStatus status);
}
