package com.octopus.kettlex.runtime.executor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.octopus.kettlex.core.exception.KettleXJSONException;
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
  }

  public DefaultExecutor(String configJson) {
    this.taskGroup =
        JsonUtil.fromJson(configJson, new TypeReference<TaskGroup>() {})
            .orElseThrow(() -> new KettleXJSONException("parse json error"));
    steps = new ArrayList<>(taskGroup.getSteps().size());
  }

  @Override
  public void executor() {
    processReaders();
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
    for (Reader<?> reader : readers) {}
  }
}
