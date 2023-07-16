package com.octopus.operators.kettlex.runtime.monitor;

import com.octopus.operators.kettlex.core.management.Communication;
import com.octopus.operators.kettlex.core.management.ExecutionStatus;
import com.octopus.operators.kettlex.runtime.config.TaskGroup;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;

public class TaskGroupContainerCommunicator {
  private final TaskGroup taskGroup;

  @Getter private Communication taskGroupCommunication;
  private final Map<String, Communication> taskCommunicationMap;

  public TaskGroupContainerCommunicator(TaskGroup taskGroup) {
    this.taskGroup = taskGroup;
    this.taskGroupCommunication = new Communication();
    this.taskCommunicationMap =
        taskGroup.getSteps().stream()
            .collect(
                Collectors.toMap(
                    communication -> communication.getStepContext().getStepName(),
                    communication -> communication.getStepContext().getCommunication()));
  }

  public Communication collectFromTask() {
    Communication communication = new Communication();
    communication.markStatus(ExecutionStatus.SUCCEEDED);

    for (Communication taskCommunication : this.taskCommunicationMap.values()) {
      communication.mergeStateFrom(taskCommunication);
    }
    this.taskGroupCommunication = communication;
    return communication;
  }
}
