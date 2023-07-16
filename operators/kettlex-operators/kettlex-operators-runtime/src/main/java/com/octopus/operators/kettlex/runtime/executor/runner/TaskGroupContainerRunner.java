package com.octopus.operators.kettlex.runtime.executor.runner;

import com.octopus.operators.kettlex.core.exception.KettleXTransException;
import com.octopus.operators.kettlex.core.management.Communication;
import com.octopus.operators.kettlex.core.management.ExecutionStatus;
import com.octopus.operators.kettlex.runtime.container.TaskGroupContainer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TaskGroupContainerRunner implements Runner {

  public final TaskGroupContainer taskGroupContainer;
  @Getter private Communication communication;

  public TaskGroupContainerRunner(
      TaskGroupContainer taskGroupContainer, Communication communication) {
    this.taskGroupContainer = taskGroupContainer;
    this.communication = communication;
  }

  @Override
  public void run() {
    try {
      Thread.currentThread()
          .setName(String.format("taskGroup-%s", this.taskGroupContainer.getContainerName()));
      communication.markStatus(ExecutionStatus.SUBMITTING);
      this.taskGroupContainer.init();
      communication.markStatus(ExecutionStatus.WAITING);
      communication.markStatus(ExecutionStatus.RUNNING);
      this.taskGroupContainer.start();
      communication.markStatus(ExecutionStatus.SUCCEEDED);
    } catch (Throwable e) {
      communication.markStatus(ExecutionStatus.FAILED);
      log.error("task group run error.", e);
      throw new KettleXTransException("task group run error.", e);
    }
  }

  @Override
  public void mark(ExecutionStatus status) {
    communication.markStatus(status);
  }

  @Override
  public void shutdown() {
    taskGroupContainer.stop();
  }

  @Override
  public void destroy() {
    taskGroupContainer.destroy();
  }
}
