package com.octopus.kettlex.runtime;

import com.octopus.kettlex.core.provider.StepConfigStepCombination;
import com.octopus.kettlex.core.provider.StepProviderResolver;
import com.octopus.kettlex.core.steps.config.TaskConfiguration;
import com.octopus.kettlex.core.utils.YamlUtil;
import com.octopus.kettlex.runtime.executor.Scheduler;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

@Slf4j
public class Engine {

  public static void main(String[] args) {
    String configBase64 = args[0];
    log.info("spark runtime instance base64: {}", configBase64);
    Engine engine = new Engine();
    engine.start(configBase64);
  }

  public void start(String configBase64) {
    TaskConfiguration taskConfiguration = new TaskConfiguration();
    YamlUtil.toYamlNode(Base64.getDecoder().decode(configBase64.getBytes(StandardCharsets.UTF_8)))
        .ifPresent(jsonNode -> taskConfiguration.loadYaml(jsonNode.asText()));
    StepProviderResolver stepProviderResolver = StepProviderResolver.getInstance();
    List<StepConfigStepCombination> stepConfigStepCombinations = new ArrayList<>();
    List<StepConfigStepCombination> readerConfigReaderCombinations =
        fillStepConfigStepCombination(taskConfiguration.getReaders(), stepProviderResolver);
    List<StepConfigStepCombination> transforConfigTransformCombinations =
        fillStepConfigStepCombination(taskConfiguration.getTransforms(), stepProviderResolver);
    List<StepConfigStepCombination> writerConfigWriterCombinations =
        fillStepConfigStepCombination(taskConfiguration.getWriters(), stepProviderResolver);
    if (CollectionUtils.isNotEmpty(readerConfigReaderCombinations)) {
      stepConfigStepCombinations.addAll(stepConfigStepCombinations);
    }
    if (CollectionUtils.isNotEmpty(transforConfigTransformCombinations)) {
      stepConfigStepCombinations.addAll(transforConfigTransformCombinations);
    }
    if (CollectionUtils.isNotEmpty(writerConfigWriterCombinations)) {
      stepConfigStepCombinations.addAll(writerConfigWriterCombinations);
    }

    Scheduler scheduler = new Scheduler();
    scheduler.startTaskGroup(taskConfiguration);
  }

  public TaskConfiguration buildTaskConfiguration(String configBase64) {
    TaskConfiguration taskConfiguration = new TaskConfiguration();
    YamlUtil.toYamlNode(Base64.getDecoder().decode(configBase64.getBytes(StandardCharsets.UTF_8)))
        .ifPresent(jsonNode -> taskConfiguration.loadYaml(jsonNode.toString()));
    StepProviderResolver stepProviderResolver = StepProviderResolver.getInstance();
    List<StepConfigStepCombination> stepConfigStepCombinations = new ArrayList<>();
    List<StepConfigStepCombination> readerConfigReaderCombinations =
        fillStepConfigStepCombination(taskConfiguration.getReaders(), stepProviderResolver);
    List<StepConfigStepCombination> transforConfigTransformCombinations =
        fillStepConfigStepCombination(taskConfiguration.getTransforms(), stepProviderResolver);
    List<StepConfigStepCombination> writerConfigWriterCombinations =
        fillStepConfigStepCombination(taskConfiguration.getWriters(), stepProviderResolver);
    if (CollectionUtils.isNotEmpty(readerConfigReaderCombinations)) {
      stepConfigStepCombinations.addAll(stepConfigStepCombinations);
    }
    if (CollectionUtils.isNotEmpty(transforConfigTransformCombinations)) {
      stepConfigStepCombinations.addAll(transforConfigTransformCombinations);
    }
    if (CollectionUtils.isNotEmpty(writerConfigWriterCombinations)) {
      stepConfigStepCombinations.addAll(writerConfigWriterCombinations);
    }
    return taskConfiguration;
  }

  public List<StepConfigStepCombination> fillStepConfigStepCombination(
      List<Map<String, Object>> steps, StepProviderResolver stepProviderResolver) {
    List<StepConfigStepCombination> result = null;
    if (CollectionUtils.isNotEmpty(steps)) {
      result = new ArrayList<>(steps.size());
      for (Map<String, Object> step : steps) {
        String type = String.valueOf(step.get("type"));
        StepConfigStepCombination stepConfigStepCombination =
            stepProviderResolver.getStepConfigStepCombination(type);
        Class<?> stepConfigClass = stepConfigStepCombination.getStepConfigClass();
        Class<?> stepClass = stepConfigStepCombination.getStepClass();
      }
    }
    return result;
  }
}
