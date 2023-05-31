package com.octopus.kettlex.runtime;

import com.octopus.kettlex.core.channel.Channel;
import com.octopus.kettlex.core.channel.DefaultChannel;
import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.exception.KettleXStepConfigException;
import com.octopus.kettlex.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.CollectionUtils;

public class TaskCombination {

  private final TaskConfiguration configuration;
  private final List<String> outputs = new ArrayList<>();
  private final Map<String, String> outputIdMap = new HashMap<>();
  private final Map<String, StepConfig<?>> stepConfigMap = new HashMap<>();
  private final List<StepLink> links = new ArrayList<>();
  private final Map<String, List<StepConfig<?>>> childSteps = new HashMap<>();
  private final Map<String, StepConfig<?>> parentSteps = new HashMap<>();

  public TaskCombination(TaskConfiguration configuration) {
    this.configuration = configuration;
  }

  public void combi() {
    String taskId = configuration.getTaskId();
    List<ReaderConfig<?>> readers = configuration.getReaders();
    if (CollectionUtils.isEmpty(readers)) {
      throw new KettleXException("reader config cannot be null");
    }

    validateIdNameUnique();
    buildStepLinks();

    // 处理ReaderConfig
    for (ReaderConfig<?> reader : readers) {
      String output = reader.getOutput();
      if (outputs.contains(output)) {
        throw new KettleXException(
            String.format(
                "output must be unique. outputs: [%s], current output:[%s]", outputs, output));
      }
      outputs.add(output);
      outputIdMap.put(output, reader.getId());
    }
    List<TransformationConfig<?>> transformations = configuration.getTransformations();
    if (CollectionUtils.isNotEmpty(transformations)) {
      for (TransformationConfig<?> transformation : transformations) {
        String output = transformation.getOutput();
        if (outputs.contains(output)) {
          throw new KettleXException(
              String.format(
                  "output must be unique. outputs: [%s], current output:[%s]", outputs, output));
        }
        outputs.add(output);
      }
    }

    List<WriterConfig<?>> writers = configuration.getWriters();
    if (CollectionUtils.isNotEmpty(writers)) {
      for (WriterConfig<?> writerConfig : writers) {
        String input = writerConfig.getInput();
        if (!outputs.contains(input)) {
          throw new KettleXException(
              String.format(
                  "cannot find this input from outputs, outputs: [%s], current input:[%s]",
                  outputs, input));
        }
      }
    }

    for (StepLink link : links) {
      String from = link.getFrom();
      List<StepConfig<?>> stepConfigs = childSteps.get(from);
    }
  }

  private void buildStepLinks() {

  }

  private void validateIdNameUnique() {
    List<ReaderConfig<?>> readers = configuration.getReaders();
    List<TransformationConfig<?>> transformations = configuration.getTransformations();
    List<WriterConfig<?>> writers = configuration.getWriters();
    int size =
        readers.size()
            + (CollectionUtils.isNotEmpty(transformations) ? transformations.size() : 0)
            + (CollectionUtils.isNotEmpty(writers) ? writers.size() : 0);
    List<StepConfig<?>> stepConfigs = new ArrayList<>(size);
    stepConfigs.addAll(readers);
    if (CollectionUtils.isNotEmpty(transformations)) {
      stepConfigs.addAll(transformations);
    }
    if (CollectionUtils.isNotEmpty(writers)) {
      stepConfigs.addAll(writers);
    }
    for (StepConfig<?> stepConfig : stepConfigs) {
      String name = stepConfig.getName();
      if (stepConfigMap.containsKey(name)) {
        throw new KettleXStepConfigException("Duplicate step name. [" + name + "]");
      }
      stepConfigMap.put(name, stepConfig);
    }
  }

  private String getChannelId(String from, String to) {
    return String.format("%s->%s", from, to);
  }

  public void findChildSteps(String stepName) {}
}
