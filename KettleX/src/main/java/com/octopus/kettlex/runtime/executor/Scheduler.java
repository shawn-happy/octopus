package com.octopus.kettlex.runtime.executor;

import com.octopus.kettlex.model.TaskConfiguration;
import com.octopus.kettlex.runtime.TaskGroup;
import com.octopus.kettlex.runtime.container.TaskGroupContainer;
import com.octopus.kettlex.runtime.executor.runner.TaskGroupContainerRunner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Scheduler {

  private ExecutorService taskGroupContainerExecutorService;

  public Scheduler() {}

  public void startTaskGroup(TaskConfiguration configuration) {
    TaskGroup taskGroup = new TaskGroup(configuration);
    TaskGroupContainer container = new TaskGroupContainer(taskGroup);
    TaskGroupContainerRunner taskGroupContainerRunner = new TaskGroupContainerRunner(container);
    taskGroupContainerExecutorService = Executors.newFixedThreadPool(taskGroup.size());
    taskGroupContainerExecutorService.submit(taskGroupContainerRunner);
    this.taskGroupContainerExecutorService.shutdown();
  }
}