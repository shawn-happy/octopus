package com.octopus.operators.kettlex.runtime.monitor;

import com.octopus.operators.kettlex.core.management.Communication;
import com.octopus.operators.kettlex.core.management.ExecutionStatus;
import com.octopus.operators.kettlex.runtime.config.TaskGroup;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;

public class TaskGroupContainerCommunicator implements ContainerCommunicator {
  private long lastReportTime = System.currentTimeMillis();
  private final VMInfo vmInfo = VMInfo.getVmInfo();

  @Getter private Communication taskGroupCommunication;
  private final Map<String, Communication> taskCommunicationMap;

  public TaskGroupContainerCommunicator(TaskGroup taskGroup) {
    this.taskGroupCommunication = new Communication();
    this.taskCommunicationMap =
        taskGroup.getSteps().stream()
            .collect(
                Collectors.toMap(
                    communication -> communication.getStepContext().getStepName(),
                    communication -> communication.getStepContext().getCommunication()));
    collect();
  }

  @Override
  public void reportVmInfo() {
    long now = System.currentTimeMillis();
    // 每5分钟打印一次
    if (now - lastReportTime >= 300000) {
      // 当前仅打印
      if (vmInfo != null) {
        vmInfo.getDelta(true);
      }
      lastReportTime = now;
    }
  }

  @Override
  public void collect() {
    Communication communication = new Communication();
    communication.markStatus(ExecutionStatus.SUCCEEDED);

    for (Communication taskCommunication : this.taskCommunicationMap.values()) {
      ExecutionStatus status = communication.getStatus();
    }

    this.taskGroupCommunication = communication;
  }

  @Override
  public Communication getCollectCommunication() {
    collect();
    return this.taskGroupCommunication;
  }

  @Override
  public Communication getStepCommunication(String stepId) {
    return taskCommunicationMap.get(stepId);
  }
}
