package com.octopus.kettlex.runtime;

import com.octopus.kettlex.core.channel.DefaultChannel;
import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.exception.KettleXStepConfigException;
import com.octopus.kettlex.model.ReaderConfig;
import com.octopus.kettlex.model.StepConfig;
import com.octopus.kettlex.model.TaskConfiguration;
import com.octopus.kettlex.model.TransformationConfig;
import com.octopus.kettlex.model.WriterConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;

public class TaskCombination {

  private final TaskConfiguration configuration;
  private final Map<String, StepConfig<?>> stepConfigMap = new HashMap<>();
  private final List<StepLink> stepLinks = new ArrayList<>();
  private Map<String, StepLink> stepLinkMap = new HashMap<>();

  public TaskCombination(TaskConfiguration configuration) {
    this.configuration = configuration;
    combine();
  }

  private void combine() {
    String taskId = configuration.getTaskId();
    List<ReaderConfig<?>> readers = configuration.getReaders();
    if (CollectionUtils.isEmpty(readers)) {
      throw new KettleXException("reader config cannot be null");
    }

    // 用于验证id, name的唯一性
    List<String> ids = new ArrayList<>();
    List<String> names = new ArrayList<>();
    // 用于记录output step映射
    Map<String, String> outputStepMap = new HashMap<>();
    for (ReaderConfig<?> readerConfig : readers) {
      readerConfig.verify();
      String id = readerConfig.getId();
      if (ids.contains(id)) {
        throw new KettleXStepConfigException("Duplicate step id. [" + id + "]");
      }
      ids.add(id);

      String name = readerConfig.getName();
      if (names.contains(name)) {
        throw new KettleXStepConfigException("Duplicate step name. [" + name + "]");
      }
      names.add(name);

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
        String id = transformation.getId();
        if (ids.contains(id)) {
          throw new KettleXStepConfigException("Duplicate step id. [" + id + "]");
        }
        ids.add(id);

        String name = transformation.getName();
        if (names.contains(name)) {
          throw new KettleXStepConfigException("Duplicate step name. [" + name + "]");
        }
        names.add(name);

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
        StepLink stepLink =
            StepLink.builder()
                .from(fromStep)
                .fromStepConfig(stepConfigMap.get(fromStep))
                .to(toStep)
                .toStepConfig(stepConfigMap.get(toStep))
                .channel(
                    new DefaultChannel(
                        configuration.getRuntimeConfig().getChannelCapcacity(),
                        getChannelId(fromStep, toStep)))
                .build();
        stepLinks.add(stepLink);
      }
    }

    List<WriterConfig<?>> writers = configuration.getWriters();
    if (CollectionUtils.isNotEmpty(writers)) {
      for (WriterConfig<?> writerConfig : writers) {
        writerConfig.verify();
        String id = writerConfig.getId();
        if (ids.contains(id)) {
          throw new KettleXStepConfigException("Duplicate step id. [" + id + "]");
        }
        ids.add(id);

        String name = writerConfig.getName();
        if (names.contains(name)) {
          throw new KettleXStepConfigException("Duplicate step name. [" + name + "]");
        }
        names.add(name);

        String input = writerConfig.getInput();
        if (!outputStepMap.containsKey(input)) {
          throw new KettleXStepConfigException(
              String.format(
                  "cannot find this input from outputs, outputs: [%s], current input:[%s]",
                  outputStepMap, input));
        }
        stepConfigMap.put(name, writerConfig);

        String fromStep = outputStepMap.get(input);
        StepLink stepLink =
            StepLink.builder()
                .from(fromStep)
                .fromStepConfig(stepConfigMap.get(fromStep))
                .to(name)
                .toStepConfig(stepConfigMap.get(name))
                .channel(
                    new DefaultChannel(
                        configuration.getRuntimeConfig().getChannelCapcacity(),
                        getChannelId(fromStep, name)))
                .build();
        stepLinks.add(stepLink);
      }
    }

    stepLinkMap =
        stepLinks.stream()
            .collect(
                Collectors.toMap(
                    (stepLink -> getChannelId(stepLink.getFrom(), stepLink.getTo())),
                    stepLink -> stepLink));

    // for gc
    ids = null;
    names = null;
    outputStepMap = null;
  }

  private String getChannelId(String from, String to) {
    return String.format("%s->%s", from, to);
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

    return stepLinks.stream()
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
        stepLinks.stream()
            .filter(stepLink -> stepName.equals(stepLink.getTo()))
            .findFirst()
            .orElse(null);
    if (link == null) {
      return null;
    }
    return link.getFromStepConfig();
  }

  public Map<String, StepLink> getStepLinks() {
    return stepLinkMap;
  }

  public StepLink getStepLink(String from, String to) {
    return stepLinkMap.get(getChannelId(from, to));
  }
}
