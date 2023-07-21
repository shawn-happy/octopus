package com.octopus.operators.kettlex.runtime.executor;

import com.octopus.operators.kettlex.runtime.config.JobConfiguration;
import com.octopus.operators.kettlex.runtime.config.TaskGroup;
import com.octopus.operators.kettlex.runtime.container.TaskGroupContainer;
import com.octopus.operators.kettlex.runtime.executor.runner.TaskGroupContainerRunner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Scheduler {

  private ExecutorService taskGroupContainerExecutorService;

  public Scheduler() {}

  public void startTaskGroup(JobConfiguration configuration) throws Exception {
    TaskGroup taskGroup = new TaskGroup(configuration);
    TaskGroupContainer container = new TaskGroupContainer(taskGroup);
    TaskGroupContainerRunner taskGroupContainerRunner = new TaskGroupContainerRunner(container);
    taskGroupContainerExecutorService = Executors.newFixedThreadPool(taskGroup.size() * 3);
    taskGroupContainerExecutorService.submit(taskGroupContainerRunner);
    Thread.sleep(10 * 1000);
    this.taskGroupContainerExecutorService.shutdown();

    container.getStatus();
  }
}
