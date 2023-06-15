package com.octopus.kettlex.runtime.executor;

import com.octopus.kettlex.runtime.config.JobConfiguration;
import com.octopus.kettlex.runtime.config.TaskGroup;
import com.octopus.kettlex.runtime.container.TaskGroupContainer;
import com.octopus.kettlex.runtime.executor.runner.TaskGroupContainerRunner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Scheduler {

  private ExecutorService taskGroupContainerExecutorService;

  public Scheduler() {}

  public void startTaskGroup(JobConfiguration configuration) throws Exception {
    TaskGroup taskGroup = new TaskGroup(configuration);
    TaskGroupContainer container = new TaskGroupContainer(taskGroup);
    TaskGroupContainerRunner taskGroupContainerRunner = new TaskGroupContainerRunner(container);
    taskGroupContainerExecutorService = Executors.newFixedThreadPool(taskGroup.size());
    taskGroupContainerExecutorService.submit(taskGroupContainerRunner);
    TimeUnit.SECONDS.sleep(1);
    this.taskGroupContainerExecutorService.shutdown();
  }
}
