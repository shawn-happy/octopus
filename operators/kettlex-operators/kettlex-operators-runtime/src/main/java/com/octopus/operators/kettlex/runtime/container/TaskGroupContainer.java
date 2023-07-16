package com.octopus.operators.kettlex.runtime.container;

import com.octopus.operators.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.operators.kettlex.core.exception.KettleXTransException;
import com.octopus.operators.kettlex.core.listener.DefaultStepListener;
import com.octopus.operators.kettlex.core.management.Communication;
import com.octopus.operators.kettlex.core.steps.Reader;
import com.octopus.operators.kettlex.core.steps.Step;
import com.octopus.operators.kettlex.core.steps.Transform;
import com.octopus.operators.kettlex.core.steps.Writer;
import com.octopus.operators.kettlex.core.steps.config.StepConfigChannelCombination;
import com.octopus.operators.kettlex.core.utils.LoadUtil;
import com.octopus.operators.kettlex.runtime.config.TaskGroup;
import com.octopus.operators.kettlex.runtime.executor.runner.ReaderRunner;
import com.octopus.operators.kettlex.runtime.executor.runner.StepInitRunner;
import com.octopus.operators.kettlex.runtime.executor.runner.TransformRunner;
import com.octopus.operators.kettlex.runtime.executor.runner.WriterRunner;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

@Slf4j
public class TaskGroupContainer implements Container {

  private final TaskGroup taskGroup;
  @Getter private final String containerId;
  @Getter private final String containerName;
  private final List<StepConfigChannelCombination<?>> stepConfigChannelCombinations;
  private final List<Step<?>> steps;
  private TaskGroupExecutor executor;

  public TaskGroupContainer(TaskGroup taskGroup) {
    this.taskGroup = taskGroup;
    this.containerId = taskGroup.getTaskGroupId();
    this.containerName = taskGroup.getTaskGroupName();
    stepConfigChannelCombinations = taskGroup.getSteps();
    steps = new ArrayList<>(stepConfigChannelCombinations.size());
    for (StepConfigChannelCombination<?> stepConfigChannelCombination :
        stepConfigChannelCombinations) {
      steps.add(LoadUtil.loadStep(stepConfigChannelCombination.getStepConfig().getType()));
    }
    steps.forEach(
        step -> {
          step.addStepListeners(new DefaultStepListener());
        });
  }

  @Override
  public void init() {
    StepInitRunner<?>[] initThreads = new StepInitRunner[steps.size()];
    Thread[] threads = new Thread[steps.size()];
    for (int i = 0; i < steps.size(); i++) {
      initThreads[i] = new StepInitRunner(steps.get(i), stepConfigChannelCombinations.get(i));
      threads[i] =
          new Thread(
              initThreads[i],
              String.format(
                  "step_[%s]_init_thread",
                  stepConfigChannelCombinations.get(i).getStepConfig().getName()));
      threads[i].start();
    }

    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (Exception ex) {
        throw new KettleXTransException("Error with init TaskGroup", ex);
      }
    }

    boolean ok = true;
    for (StepInitRunner<?> initThread : initThreads) {
      if (!initThread.isOk()) {
        ok = false;
        break;
      }
    }

    if (!ok) {
      for (StepInitRunner<?> initThread : initThreads) {
        initThread.getStep().destroy();
      }
    }

    executor = new TaskGroupExecutor(containerId, steps);
  }

  @Override
  public void start() {
    if (executor == null) {
      executor = new TaskGroupExecutor(containerId, steps);
    }
    executor.executor();
  }

  @Override
  public void stop() {}

  @Override
  public void destroy() {}

  public void getStatus() {
    stepConfigChannelCombinations.forEach(
        combination -> {
          Communication communication = combination.getStepContext().getCommunication();
          log.info("{}", communication.toString());
        });
  }

  class TaskGroupExecutor {
    private List<Thread> readerThreads;
    private List<Thread> transformThreads;
    private List<Thread> writerThreads;
    private Communication taskCommunication;

    public TaskGroupExecutor(String taskId, List<Step<?>> steps) {
      if (CollectionUtils.isEmpty(steps)) {
        throw new KettleXStepExecuteException("steps is null");
      }
      this.readerThreads =
          steps.stream()
              .filter(step -> step instanceof Reader<?>)
              .map(
                  step ->
                      new Thread(
                          new ReaderRunner((Reader<?>) step),
                          String.format("%s_%s_reader", taskId, step.getStepConfig().getId())))
              .collect(Collectors.toUnmodifiableList());

      this.transformThreads =
          steps.stream()
              .filter(step -> step instanceof Transform<?>)
              .map(
                  step ->
                      new Thread(
                          new TransformRunner((Transform<?>) step),
                          String.format(
                              "%s_%s_transformation", taskId, step.getStepConfig().getId())))
              .collect(Collectors.toUnmodifiableList());

      this.writerThreads =
          steps.stream()
              .filter(step -> step instanceof Writer<?>)
              .map(
                  step ->
                      new Thread(
                          new WriterRunner((Writer<?>) step),
                          String.format("%s_%s_writer", taskId, step.getStepConfig().getId())))
              .collect(Collectors.toUnmodifiableList());

      taskCommunication = new Communication();
    }

    public void executor() {
      for (Thread writerThread : writerThreads) {
        writerThread.start();
      }

      for (Thread writerThread : writerThreads) {
        // reader没有起来，writer不可能结束
        if (!writerThread.isAlive()) {
          throw new KettleXStepExecuteException("write error");
        }
      }

      if (CollectionUtils.isNotEmpty(transformThreads)) {
        for (Thread transformThread : transformThreads) {
          transformThread.start();
        }
        for (Thread transformThread : transformThreads) {
          // reader没有起来，transform不可能结束
          if (!transformThread.isAlive()) {
            throw new KettleXStepExecuteException("transform error");
          }
        }
      }

      for (Thread readerThread : readerThreads) {
        readerThread.start();
      }
      for (Thread readerThread : readerThreads) {
        // 这里reader可能很快结束
        if (!readerThread.isAlive()) {
          // 这里有可能出现Reader线上启动即挂情况 对于这类情况 需要立刻抛出异常
          throw new KettleXStepExecuteException("reader error");
        }
      }
    }

    public void shutdown() {
      steps.forEach(Step::shutdown);
      for (Thread thread : writerThreads) {
        if (thread.isAlive()) {
          thread.interrupt();
        }
      }
      for (Thread thread : transformThreads) {
        if (thread.isAlive()) {
          thread.interrupt();
        }
      }

      for (Thread thread : readerThreads) {
        if (thread.isAlive()) {
          thread.interrupt();
        }
      }
    }

    public boolean isFinished() {
      for (Thread thread : writerThreads) {
        if (thread.isAlive()) {
          return false;
        }
      }
      for (Thread thread : transformThreads) {
        if (thread.isAlive()) {
          return false;
        }
      }

      for (Thread thread : readerThreads) {
        if (thread.isAlive()) {
          return false;
        }
      }
      return true;
    }
  }
}
