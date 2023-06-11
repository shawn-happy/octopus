package com.octopus.kettlex.runtime.executor.runner;

import com.octopus.kettlex.core.exception.KettleXTransException;
import com.octopus.kettlex.runtime.container.TaskGroupContainer;
import com.octopus.kettlex.runtime.monitor.ExecutionStatus;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TaskGroupContainerRunner implements Runner {

  public final TaskGroupContainer taskGroupContainer;
  private ExecutionStatus status;

  public TaskGroupContainerRunner(TaskGroupContainer taskGroupContainer) {
    this.taskGroupContainer = taskGroupContainer;
  }

  @Override
  public void run() {
    try {
      Thread.currentThread()
          .setName(String.format("taskGroup-%s", this.taskGroupContainer.getContainerName()));
      this.taskGroupContainer.init();
      this.taskGroupContainer.start();
      this.status = ExecutionStatus.RUNNING;
    } catch (Throwable e) {
      this.status = ExecutionStatus.FAILED;
      log.error("task group run error.", e);
      throw new KettleXTransException("task group run error.", e);
    }
  }

  @Override
  public void mark(ExecutionStatus status) {
    this.status = status;
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
