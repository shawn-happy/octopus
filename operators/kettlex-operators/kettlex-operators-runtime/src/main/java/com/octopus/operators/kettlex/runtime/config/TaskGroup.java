package com.octopus.operators.kettlex.runtime.config;

import com.octopus.operators.kettlex.core.context.DefaultStepContext;
import com.octopus.operators.kettlex.core.exception.KettleXStepConfigException;
import com.octopus.operators.kettlex.core.management.Communication;
import com.octopus.operators.kettlex.core.row.channel.Channel;
import com.octopus.operators.kettlex.core.row.channel.DefaultChannel;
import com.octopus.operators.kettlex.core.steps.Reader;
import com.octopus.operators.kettlex.core.steps.Step;
import com.octopus.operators.kettlex.core.steps.Transform;
import com.octopus.operators.kettlex.core.steps.Writer;
import com.octopus.operators.kettlex.core.steps.config.ReaderConfig;
import com.octopus.operators.kettlex.core.steps.config.StepConfig;
import com.octopus.operators.kettlex.core.steps.config.StepConfig.StepOptions;
import com.octopus.operators.kettlex.core.steps.config.StepConfigChannelCombination;
import com.octopus.operators.kettlex.core.steps.config.TransformerConfig;
import com.octopus.operators.kettlex.core.steps.config.WriterConfig;
import com.octopus.operators.kettlex.core.utils.LoadUtil;
import com.octopus.operators.kettlex.core.utils.YamlUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

public class TaskGroup {

  private final String taskGroupId;
  private final String taskGroupName;
  private final Map<String, StepConfig<? extends StepOptions>> stepConfigMap = new HashMap<>();
  private final Map<String, StepConfigChannelCombination<? extends StepConfig<?>>>
      stepConfigChannelCombinationMap = new HashMap<>();
  private final Map<String, StepLink> stepLinkMap = new HashMap<>();

  public TaskGroup(JobConfiguration configuration) {
    this.taskGroupId = configuration.getTaskId();
    this.taskGroupName = configuration.getTaskName();
    TaskGroupExecutorConfig config = buildTaskGroupExecutorConfig(configuration);
    combineStepLinks(config);
    combineStepChannelCombine();
  }

  public String getTaskGroupId() {
    return taskGroupId;
  }

  public String getTaskGroupName() {
    return taskGroupName;
  }

  public int size() {
    return MapUtils.isEmpty(stepConfigChannelCombinationMap)
        ? 0
        : stepConfigChannelCombinationMap.size();
  }

  public List<StepConfigChannelCombination<?>> getSteps() {
    return stepConfigChannelCombinationMap.values().stream()
        .collect(Collectors.toUnmodifiableList());
  }

  public StepConfigChannelCombination<?> getStepChannel(String name) {
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

  public TaskGroupExecutorConfig buildTaskGroupExecutorConfig(JobConfiguration configuration) {
    TaskGroupExecutorConfig config = new TaskGroupExecutorConfig();
    List<Map<String, Object>> readers = configuration.getReaders();
    if (CollectionUtils.isEmpty(readers)) {
      throw new KettleXStepConfigException("reader plugins cannot be null");
    }
    List<ReaderConfig<?>> readerConfigs = new ArrayList<>(readers.size());
    for (Map<String, Object> reader : readers) {
      String type = String.valueOf(reader.get("type"));
      Step<?> step = LoadUtil.loadStep(type);
      StepConfig<?> stepConfig = LoadUtil.loadStepConfig(type);
      if (!(step instanceof Reader<?> && stepConfig instanceof ReaderConfig<?>)) {
        throw new KettleXStepConfigException(String.format("step [%s] is not reader plugin", type));
      }
      stepConfig.loadYaml(YamlUtil.toYaml(reader).orElse(null));
      ReaderConfig<?> readerConfig = (ReaderConfig<?>) stepConfig;
      if (stepConfigMap.containsKey(stepConfig.getName())) {
        throw new KettleXStepConfigException("Duplicate step name. [" + stepConfig.getName() + "]");
      }
      stepConfigMap.put(stepConfig.getName(), stepConfig);
      readerConfigs.add(readerConfig);
    }
    config.setReaders(readerConfigs);

    List<Map<String, Object>> transforms = configuration.getTransforms();
    if (CollectionUtils.isNotEmpty(transforms)) {
      List<TransformerConfig<?>> transformerConfigs = new ArrayList<>(transforms.size());
      for (Map<String, Object> transform : transforms) {
        String type = String.valueOf(transform.get("type"));
        Step<?> step = LoadUtil.loadStep(type);
        StepConfig<?> stepConfig = LoadUtil.loadStepConfig(type);
        if (!(step instanceof Transform<?> && stepConfig instanceof TransformerConfig<?>)) {
          throw new KettleXStepConfigException(
              String.format("step [%s] is not transformer plugin", type));
        }
        stepConfig.loadYaml(YamlUtil.toYaml(transform).orElse(null));
        if (stepConfigMap.containsKey(stepConfig.getName())) {
          throw new KettleXStepConfigException(
              "Duplicate step name. [" + stepConfig.getName() + "]");
        }
        stepConfigMap.put(stepConfig.getName(), stepConfig);
        transformerConfigs.add((TransformerConfig<?>) stepConfig);
      }
      config.setTransforms(transformerConfigs);
    }

    List<Map<String, Object>> writers = configuration.getWriters();
    if (CollectionUtils.isNotEmpty(writers)) {
      List<WriterConfig<?>> writerConfigs = new ArrayList<>(writers.size());
      for (Map<String, Object> writer : writers) {
        String type = String.valueOf(writer.get("type"));
        Step<?> step = LoadUtil.loadStep(type);
        StepConfig<?> stepConfig = LoadUtil.loadStepConfig(type);
        if (!(step instanceof Writer<?> && stepConfig instanceof WriterConfig<?>)) {
          throw new KettleXStepConfigException(
              String.format("step [%s] is not writer plugin", type));
        }
        stepConfig.loadYaml(YamlUtil.toYaml(writer).orElse(null));
        if (stepConfigMap.containsKey(stepConfig.getName())) {
          throw new KettleXStepConfigException(
              "Duplicate step name. [" + stepConfig.getName() + "]");
        }
        stepConfigMap.put(stepConfig.getName(), stepConfig);
        writerConfigs.add((WriterConfig<?>) stepConfig);
      }
      config.setWriters(writerConfigs);
    }
    config.setRuntimeConfig(configuration.getRuntimeConfig());
    return config;
  }

  private void combineStepLinks(TaskGroupExecutorConfig config) {
    List<ReaderConfig<?>> readers = config.getReaders();
    // 用于记录output step映射
    Map<String, String> outputStepMap = new HashMap<>();
    for (ReaderConfig<?> readerConfig : readers) {
      readerConfig.verify();
      String name = readerConfig.getName();

      String output = readerConfig.getOutput();
      if (outputStepMap.containsKey(output)) {
        throw new KettleXStepConfigException(
            String.format(
                "output must be unique. outputs: [%s], current output:[%s]",
                outputStepMap, output));
      }
      outputStepMap.put(output, name);
    }

    List<TransformerConfig<?>> transformations = config.getTransforms();
    if (CollectionUtils.isNotEmpty(transformations)) {
      // transformation output也有可能是transformation或者writer的input，
      // 所以先记录transformation的output，避免由于顺序问题，而找不到input
      for (TransformerConfig<?> transformation : transformations) {
        transformation.verify();
        String name = transformation.getName();
        String output = transformation.getOutput();
        if (outputStepMap.containsKey(output)) {
          throw new KettleXStepConfigException(
              String.format(
                  "output must be unique. outputs: [%s], current output:[%s]",
                  outputStepMap, output));
        }
        outputStepMap.put(output, name);
      }

      for (TransformerConfig<?> transformation : transformations) {
        String input = transformation.getInput();
        if (!outputStepMap.containsKey(input)) {
          throw new KettleXStepConfigException(
              String.format(
                  "cannot find this input from outputs, outputs: [%s], current input:[%s]",
                  outputStepMap, input));
        }

        String fromStep = outputStepMap.get(input);
        String toStep = transformation.getName();
        createStepLink(fromStep, toStep, config);
      }
    }

    List<WriterConfig<?>> writers = config.getWriters();
    if (CollectionUtils.isNotEmpty(writers)) {
      for (WriterConfig<?> writerConfig : writers) {
        writerConfig.verify();

        String name = writerConfig.getName();

        String input = writerConfig.getInput();
        if (!outputStepMap.containsKey(input)) {
          throw new KettleXStepConfigException(
              String.format(
                  "cannot find this input from outputs, outputs: [%s], current input:[%s]",
                  outputStepMap, input));
        }
        String fromStep = outputStepMap.get(input);
        createStepLink(fromStep, name, config);
      }
    }
  }

  private void createStepLink(String from, String to, TaskGroupExecutorConfig config) {
    StepLink stepLink =
        StepLink.builder()
            .from(from)
            .fromStepConfig(stepConfigMap.get(from))
            .to(to)
            .toStepConfig(stepConfigMap.get(to))
            .channel(
                new DefaultChannel(
                    getChannelId(from, to), config.getRuntimeConfig().getChannelCapcacity()))
            .build();
    stepLinkMap.put(getChannelId(from, to), stepLink);
  }

  private <C extends StepConfig<?>> void combineStepChannelCombine() {
    for (String stepName : stepConfigMap.keySet()) {
      List<StepConfig<?>> childSteps = findChildSteps(stepName);
      StepConfig<?> parentStep = findParentStep(stepName);
      C stepConfig = (C) stepConfigMap.get(stepName);
      StepConfigChannelCombination<C> combination = new StepConfigChannelCombination<>();
      combination.setStepConfig(stepConfig);
      List<Channel> outputChannels =
          CollectionUtils.isEmpty(childSteps)
              ? null
              : childSteps.stream()
                  .map(sc -> getStepLink(stepName, sc.getName()).getChannel())
                  .collect(Collectors.toList());
      if (parentStep != null) {
        Channel inputChannel = getStepLink(parentStep.getName(), stepName).getChannel();
        combination.setInputChannel(inputChannel);
      }
      if (CollectionUtils.isNotEmpty(outputChannels)) {
        combination.setOutputChannels(outputChannels);
      }
      combination.setStepContext(new DefaultStepContext(stepName, new Communication()));
      stepConfigChannelCombinationMap.put(stepName, combination);
    }
  }

  private String getChannelId(String from, String to) {
    return String.format("%s->%s", from, to);
  }

  @Getter
  @Setter
  @NoArgsConstructor
  @AllArgsConstructor
  private static class TaskGroupExecutorConfig {
    private List<ReaderConfig<?>> readers;
    private List<TransformerConfig<?>> transforms;
    private List<WriterConfig<?>> writers;
    private RuntimeConfig runtimeConfig;
  }
}
