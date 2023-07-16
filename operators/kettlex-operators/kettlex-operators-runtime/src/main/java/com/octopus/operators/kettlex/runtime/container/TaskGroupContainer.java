package com.octopus.operators.kettlex.runtime.container;

import com.octopus.operators.kettlex.core.exception.KettleXStepExecuteException;
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
import org.apache.commons.lang3.ArrayUtils;

@Slf4j
public class TaskGroupContainer implements Container {

  private final TaskGroup taskGroup;
  @Getter private final String containerId;
  @Getter private final String containerName;
  private final List<StepConfigChannelCombination<?>> stepConfigChannelCombinations;
  private final List<Step<?>> steps;

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

    for (int i = 0; i < threads.length; i++) {
      try {
        threads[i].join();
      } catch (Exception ex) {
        log.error("Error with init thread: " + ex.getMessage(), ex.getMessage());
      }
    }

    boolean ok = true;
    for (StepInitRunner initThread : initThreads) {
      if (!initThread.isOk()) {
        ok = false;
        break;
      }
    }

    if (!ok) {
      for (StepInitRunner initThread : initThreads) {
        initThread.getStep().destroy();
      }
    }
  }

  @Override
  public void start() {
    TaskGroupExecutor executor = new TaskGroupExecutor(steps);
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
    private List<Reader<?>> readers;
    private Thread[] readerThreads;
    private List<Transform<?>> transforms;
    private Thread[] transformThreads;
    private List<Writer<?>> writers;
    private Thread[] writerThreads;
    private Communication taskCommunication;

    public TaskGroupExecutor(List<Step<?>> steps) {
      if (CollectionUtils.isEmpty(steps)) {
        throw new KettleXStepExecuteException("steps is null");
      }
      this.readers =
          steps.stream()
              .filter(step -> step instanceof Reader<?>)
              .map(step -> (Reader<?>) step)
              .collect(Collectors.toUnmodifiableList());

      this.transforms =
          steps.stream()
              .filter(step -> step instanceof Transform<?>)
              .map(step -> (Transform<?>) step)
              .collect(Collectors.toUnmodifiableList());

      this.writers =
          steps.stream()
              .filter(step -> step instanceof Writer<?>)
              .map(step -> (Writer<?>) step)
              .collect(Collectors.toUnmodifiableList());

      ReaderRunner[] readerRunners = new ReaderRunner[readers.size()];
      readerThreads = new Thread[readers.size()];
      for (int i = 0; i < readers.size(); i++) {
        Reader<?> reader = readers.get(i);
        readerRunners[i] = new ReaderRunner(reader);
        readerThreads[i] =
            new Thread(
                readerRunners[i],
                String.format("reader_[%s]_thread", reader.getStepConfig().getName()));
      }

      if (CollectionUtils.isNotEmpty(transforms)) {
        TransformRunner[] tranformRunners = new TransformRunner[transforms.size()];
        transformThreads = new Thread[transforms.size()];
        for (int i = 0; i < transforms.size(); i++) {
          Transform<?> transform = transforms.get(i);
          tranformRunners[i] = new TransformRunner(transform);
          transformThreads[i] =
              new Thread(
                  tranformRunners[i],
                  String.format("transform_[%s]_thread", transform.getStepConfig().getName()));
        }
      }

      WriterRunner[] writerRunners = new WriterRunner[writers.size()];
      writerThreads = new Thread[writers.size()];
      for (int i = 0; i < writers.size(); i++) {
        Writer<?> writer = writers.get(i);
        writerRunners[i] = new WriterRunner(writer);
        writerThreads[i] =
            new Thread(
                writerRunners[i],
                String.format("writer_[%s]_thread", writer.getStepConfig().getName()));
      }

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

      if (ArrayUtils.isNotEmpty(transformThreads)) {
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

      waitUntilFinished(100);
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

    private void waitUntilFinished(final long waitMillis) {

      if (!isFinished()) {

        for (int i = 0; i < 100; i++) {
          if (isFinished()) {
            break;
          }
          try {
            Thread.sleep(waitMillis);
          } catch (Exception e) {
            e.printStackTrace();
            break;
          }
        }
      }
    }
  }
}
