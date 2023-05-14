package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.statistics.ExecutionState;

public interface StepContext {

  void setStatus(ExecutionState status);

  void initCommunication();

  void reportStepCommunication();
}
