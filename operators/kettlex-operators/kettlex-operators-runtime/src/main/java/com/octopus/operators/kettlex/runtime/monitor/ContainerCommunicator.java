package com.octopus.operators.kettlex.runtime.monitor;

import com.octopus.operators.kettlex.core.management.Communication;

public interface ContainerCommunicator {
  void collect();

  Communication getCollectCommunication();

  Communication getStepCommunication(String stepId);

  void reportVmInfo();
}
