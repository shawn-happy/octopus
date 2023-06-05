package com.octopus.kettlex.runtime;

import com.octopus.kettlex.core.channel.Channel;
import com.octopus.kettlex.core.channel.DefaultChannel;
import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.exception.KettleXStepConfigException;
import com.octopus.kettlex.core.steps.StepConfigChannelCombination;
import com.octopus.kettlex.model.ReaderConfig;
import com.octopus.kettlex.model.StepConfig;
import com.octopus.kettlex.model.TaskConfiguration;
import com.octopus.kettlex.model.TransformationConfig;
import com.octopus.kettlex.model.WriterConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;

public class TaskGroup {

  private final TaskConfiguration configuration;
  private final Map<String, StepConfig<?>> stepConfigMap = new HashMap<>();
  private final Map<String, StepConfigChannelCombination> stepConfigChannelCombinationMap =
      new HashMap<>();
  private final Map<String, StepLink> stepLinkMap = new HashMap<>();

  public TaskGroup(TaskConfiguration configuration) {
    this.configuration = configuration;
    combineStepLinks();
    combineStepChannelCombine();
  }

  public List<StepConfigChannelCombination> getSteps() {
    return stepConfigChannelCombinationMap.values().stream()
        .collect(Collectors.toUnmodifiableList());
  }

  public StepConfigChannelCombination getStepChannel(String name) {
    return stepConfigChannelCombinationMap.get(name);
  }

  public List<StepConfig<?>> findChildSteps(String stepName) {
    StepConfig<?> stepConfig = stepConfigMap.get(stepName);
    if (stepConfig == null) {
      throw new KettleXStepConfigException(
          String.format("step cannot find by step name. [%s]", stepName));
    }

    // 输出节点没有子节点
    if (stepConfig instanceof WriterConfig) {
      return null;
    }

    return stepLinkMap.values().stream()
        .filter(stepLink -> stepLink.getFrom().equals(stepName))
        .map(StepLink::getToStepConfig)
        .collect(Collectors.toUnmodifiableList());
  }

  public StepConfig<?> findParentStep(String stepName) {
    StepConfig<?> stepConfig = stepConfigMap.get(stepName);
    if (stepConfig == null) {
      throw new KettleXStepConfigException(
          String.format("step cannot find by step name. [%s]", stepName));
    }

    // 输入节点没有父节点
    if (stepConfig instanceof ReaderConfig) {
      return null;
    }

    // combine的时候已经保证了input只有一个，所以只会有1个父节点。
    StepLink link =
        stepLinkMap.values().stream()
            .filter(stepLink -> stepName.equals(stepLink.getTo()))
            .findFirst()
            .orElse(null);
    if (link == null) {
      return null;
    }
    return link.getFromStepConfig();
  }

  public StepLink getStepLink(String from, String to) {
    return stepLinkMap.get(getChannelId(from, to));
  }

  private void combineStepLinks() {
    String taskId = configuration.getTaskId();
    List<ReaderConfig<?>> readers = configuration.getReaders();
    if (CollectionUtils.isEmpty(readers)) {
      throw new KettleXException("reader config cannot be null");
    }

    // 用于记录output step映射
    Map<String, String> outputStepMap = new HashMap<>();
    for (ReaderConfig<?> readerConfig : readers) {
      readerConfig.verify();
      String name = readerConfig.getName();
      if (stepConfigMap.containsKey(name)) {
        throw new KettleXStepConfigException("Duplicate step name. [" + name + "]");
      }

      String output = readerConfig.getOutput();
      if (outputStepMap.containsKey(output)) {
        throw new KettleXStepConfigException(
            String.format(
                "output must be unique. outputs: [%s], current output:[%s]",
                outputStepMap, output));
      }
      outputStepMap.put(output, name);
      stepConfigMap.put(name, readerConfig);
    }

    List<TransformationConfig<?>> transformations = configuration.getTransformations();
    if (CollectionUtils.isNotEmpty(transformations)) {
      // transformation output也有可能是transformation或者writer的input，
      // 所以先记录transformation的output，避免由于顺序问题，而找不到input
      for (TransformationConfig<?> transformation : transformations) {
        transformation.verify();
        String name = transformation.getName();
        if (stepConfigMap.containsKey(name)) {
          throw new KettleXStepConfigException("Duplicate step name. [" + name + "]");
        }

        String output = transformation.getOutput();
        if (outputStepMap.containsKey(output)) {
          throw new KettleXStepConfigException(
              String.format(
                  "output must be unique. outputs: [%s], current output:[%s]",
                  outputStepMap, output));
        }
        outputStepMap.put(output, name);
      }

      for (TransformationConfig<?> transformation : transformations) {
        String input = transformation.getInput();
        if (!outputStepMap.containsKey(input)) {
          throw new KettleXStepConfigException(
              String.format(
                  "cannot find this input from outputs, outputs: [%s], current input:[%s]",
                  outputStepMap, input));
        }
        stepConfigMap.put(transformation.getName(), transformation);

        String fromStep = outputStepMap.get(input);
        String toStep = transformation.getName();
        createStepLink(fromStep, toStep);
      }
    }

    List<WriterConfig<?>> writers = configuration.getWriters();
    if (CollectionUtils.isNotEmpty(writers)) {
      for (WriterConfig<?> writerConfig : writers) {
        writerConfig.verify();

        String name = writerConfig.getName();
        if (stepConfigMap.containsKey(name)) {
          throw new KettleXStepConfigException("Duplicate step name. [" + name + "]");
        }

        String input = writerConfig.getInput();
        if (!outputStepMap.containsKey(input)) {
          throw new KettleXStepConfigException(
              String.format(
                  "cannot find this input from outputs, outputs: [%s], current input:[%s]",
                  outputStepMap, input));
        }
        stepConfigMap.put(name, writerConfig);
        String fromStep = outputStepMap.get(input);
        createStepLink(fromStep, name);
      }
    }
  }

  private void createStepLink(String from, String to) {
    StepLink stepLink =
        StepLink.builder()
            .from(from)
            .fromStepConfig(stepConfigMap.get(from))
            .to(to)
            .toStepConfig(stepConfigMap.get(to))
            .channel(
                new DefaultChannel(
                    configuration.getRuntimeConfig().getChannelCapcacity(), getChannelId(from, to)))
            .build();
    stepLinkMap.put(getChannelId(from, to), stepLink);
  }

  private void combineStepChannelCombine() {
    for (String stepName : stepConfigMap.keySet()) {
      List<StepConfig<?>> childSteps = findChildSteps(stepName);
      StepConfig<?> parentStep = findParentStep(stepName);
      StepConfig<?> stepConfig = stepConfigMap.get(stepName);
      StepConfigChannelCombination combination = new StepConfigChannelCombination();
      combination.setStepConfig(stepConfig);
      List<Channel> outputChannels =
          CollectionUtils.isEmpty(childSteps)
              ? null
              : childSteps.stream()
                  .map(sc -> getStepLink(stepName, sc.getName()).getChannel())
                  .collect(Collectors.toList());
      Channel inputChannel = getStepLink(parentStep.getName(), stepName).getChannel();
      combination.setOutputChannels(outputChannels);
      combination.setInputChannel(inputChannel);
      stepConfigChannelCombinationMap.put(stepName, combination);
    }
  }

  private String getChannelId(String from, String to) {
    return String.format("%s->%s", from, to);
  }
}
