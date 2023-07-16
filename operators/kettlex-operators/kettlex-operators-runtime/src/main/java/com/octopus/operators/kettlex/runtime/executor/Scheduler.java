package com.octopus.operators.kettlex.runtime.executor;

import com.octopus.operators.kettlex.core.management.Communication;
import com.octopus.operators.kettlex.runtime.config.JobConfiguration;
import com.octopus.operators.kettlex.runtime.config.TaskGroup;
import com.octopus.operators.kettlex.runtime.container.TaskGroupContainer;
import com.octopus.operators.kettlex.runtime.executor.runner.TaskGroupContainerRunner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Scheduler {

  private ExecutorService taskGroupContainerExecutorService;

  public Scheduler() {}

  public void startTaskGroup(JobConfiguration configuration) throws Exception {
    TaskGroup taskGroup = new TaskGroup(configuration);
    TaskGroupContainer container = new TaskGroupContainer(taskGroup);
    TaskGroupContainerRunner taskGroupContainerRunner =
        new TaskGroupContainerRunner(container, new Communication());
    taskGroupContainerExecutorService = Executors.newFixedThreadPool(taskGroup.size() * 3);
    taskGroupContainerExecutorService.submit(taskGroupContainerRunner);
    this.taskGroupContainerExecutorService.shutdown();

    try {
      int i = 0;
      while (true) {
        if (i == 2) {
          container.getStatus();
          break;
        }
        i++;
        Thread.sleep(100000);
        container.getStatus();
      }
    } catch (InterruptedException e) {
      // 以 failed 状态退出
      e.printStackTrace();
    }
  }
}
