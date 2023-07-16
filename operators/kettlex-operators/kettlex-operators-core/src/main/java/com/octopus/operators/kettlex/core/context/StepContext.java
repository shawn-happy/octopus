package com.octopus.operators.kettlex.core.context;

import com.octopus.operators.kettlex.core.management.Communication;

public interface StepContext {

  String getStepName();

  Communication getCommunication();

  void updateCommunication();

  void reportCommunication();
}
