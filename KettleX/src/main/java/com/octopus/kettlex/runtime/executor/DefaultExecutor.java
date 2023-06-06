package com.octopus.kettlex.runtime.executor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.octopus.kettlex.core.exception.KettleXJSONException;
import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.steps.Reader;
import com.octopus.kettlex.core.steps.Step;
import com.octopus.kettlex.core.steps.StepConfigChannelCombination;
import com.octopus.kettlex.core.steps.StepFactory;
import com.octopus.kettlex.core.steps.Transform;
import com.octopus.kettlex.core.steps.Writer;
import com.octopus.kettlex.core.utils.JsonUtil;
import com.octopus.kettlex.runtime.TaskGroup;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;

@Slf4j
public class DefaultExecutor implements Executor {
  private final TaskGroup taskGroup;
  private List<Step<?>> steps;
  private List<Reader<?>> readers;
  private Thread[] readerThreads;
  private List<Transform<?>> transforms;
  private Thread[] transformThreads;
  private List<Writer<?>> writers;
  private Thread[] writerThreads;

  public DefaultExecutor(TaskGroup taskGroup) {
    this.taskGroup = taskGroup;
    this.steps = new ArrayList<>(taskGroup.getSteps().size());
  }

  public DefaultExecutor(String configJson) {
    this.taskGroup =
        JsonUtil.fromJson(configJson, new TypeReference<TaskGroup>() {})
            .orElseThrow(() -> new KettleXJSONException("parse json error"));
    steps = new ArrayList<>(taskGroup.getSteps().size());
  }

  @Override
  public void executor() {
    prepareExecution();
    processReaders();
    processTransformations();
    processWriters();

    for (Thread writerThread : writerThreads) {
      // reader没有起来，writer不可能结束
      if (!writerThread.isAlive()) {
        throw new KettleXStepExecuteException("write error");
      }
    }

    if (ArrayUtils.isNotEmpty(transformThreads)) {
      for (Thread transformThread : transformThreads) {
        // reader没有起来，transform不可能结束
        if (!transformThread.isAlive()) {
          throw new KettleXStepExecuteException("transform error");
        }
      }
    }

    for (Thread readerThread : readerThreads) {
      // 这里reader可能很快结束
      if (!readerThread.isAlive()) {
        // 这里有可能出现Reader线上启动即挂情况 对于这类情况 需要立刻抛出异常
        throw new KettleXStepExecuteException("reader error");
      }
    }
  }

  private void prepareExecution() {
    createSteps();
    initAllSteps();
  }

  /** 创建所有的step */
  private void createSteps() {
    List<StepConfigChannelCombination> stepConfigChannelCombinations = taskGroup.getSteps();
    for (StepConfigChannelCombination stepConfigChannelCombination :
        stepConfigChannelCombinations) {
      steps.add(StepFactory.createStep(stepConfigChannelCombination));
    }
    readers =
        steps.stream()
            .filter(step -> step instanceof Reader<?>)
            .map(step -> (Reader<?>) step)
            .collect(Collectors.toUnmodifiableList());
    transforms =
        steps.stream()
            .filter(step -> step instanceof Transform<?>)
            .map(step -> (Transform<?>) step)
            .collect(Collectors.toUnmodifiableList());

    writers =
        steps.stream()
            .filter(step -> step instanceof Writer<?>)
            .map(step -> (Writer<?>) step)
            .collect(Collectors.toUnmodifiableList());
  }

  private void initAllSteps() {
    StepInitRunner[] initThreads = new StepInitRunner[steps.size()];
    Thread[] threads = new Thread[steps.size()];
    for (int i = 0; i < steps.size(); i++) {
      initThreads[i] = new StepInitRunner(steps.get(i));
      threads[i] =
          new Thread(
              initThreads[i],
              String.format("step_[%s]_init_thread", steps.get(i).getStepConfig().getName()));
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

  private void processReaders() {
    ReaderRunner[] readerRunners = new ReaderRunner[readers.size()];
    readerThreads = new Thread[readers.size()];
    for (int i = 0; i < readers.size(); i++) {
      Reader<?> reader = readers.get(i);
      readerRunners[i] = new ReaderRunner(reader);
      readerThreads[i] =
          new Thread(
              readerRunners[i],
              String.format("reader_[%s]_thread", reader.getStepConfig().getName()));
      readerThreads[i].start();
    }
  }

  private void processTransformations() {
    if (CollectionUtils.isEmpty(transforms)) {
      return;
    }
    TransformRunner[] tranformRunners = new TransformRunner[transforms.size()];
    transformThreads = new Thread[transforms.size()];
    for (int i = 0; i < transforms.size(); i++) {
      Transform<?> transform = transforms.get(i);
      tranformRunners[i] = new TransformRunner(transform);
      transformThreads[i] =
          new Thread(
              tranformRunners[i],
              String.format("transform_[%s]_thread", transform.getStepConfig().getName()));
      transformThreads[i].start();
    }
  }

  private void processWriters() {
    if (CollectionUtils.isEmpty(writers)) {
      return;
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
      writerThreads[i].start();
    }
  }
}
