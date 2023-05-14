package com.octopus.kettlex.core.statistics;

import com.octopus.kettlex.core.executor.TransExecutor;
import com.octopus.kettlex.core.steps.Step;

public class Communicator {

  private final TransExecutor executor;
  private final LocalCommunicationManager localCommunicationManager;

  public Communicator(TransExecutor executor, LocalCommunicationManager localCommunicationManager) {
    this.executor = executor;
    this.localCommunicationManager = localCommunicationManager;
  }

  public Communication getStepCommunication(int stepId) {
    return localCommunicationManager.getStepCommunication(stepId);
  }

  public void registerCommunication(Step step) {
    Communication communication = new Communication();
    communication.setTimestamp(System.currentTimeMillis());
    communication.setState(ExecutionState.SUBMITTING);
    localCommunicationManager.registerStepCommunication(0, communication);
  }
}
