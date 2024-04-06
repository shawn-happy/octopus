package io.github.shawn.octopus.fluxus.engine.pipeline.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.github.shawn.octopus.fluxus.api.common.IdGenerator;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.config.SinkConfig;
import io.github.shawn.octopus.fluxus.api.config.SourceConfig;
import io.github.shawn.octopus.fluxus.api.config.StepConfig;
import io.github.shawn.octopus.fluxus.api.config.TransformConfig;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.provider.PluginRegistry;
import io.github.shawn.octopus.fluxus.api.provider.SinkProvider;
import io.github.shawn.octopus.fluxus.api.provider.SourceProvider;
import io.github.shawn.octopus.fluxus.api.provider.TransformProvider;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;

@Getter
@Setter
public class JobConfig {
  private String jobId;
  private String jobName;
  private JobMode jobMode = JobMode.BATCH;
  private RuntimeConfig runtimeConfig;
  private List<SourceConfig<?>> sources;
  private List<TransformConfig<?>> transforms;
  private SinkConfig<?> sink;
  private transient List<StepConfig> stepConfigs;

  public JobConfig() {
    this.jobId = IdGenerator.uuid();
  }

  public JobConfig(String json) {
    this.jobId = IdGenerator.uuid();
    loadJobConfig(json);
  }

  public List<StepConfig> getSteps() {
    if (CollectionUtils.isNotEmpty(stepConfigs)) {
      return stepConfigs;
    }
    stepConfigs =
        new ArrayList<>(
            sources.size() + 1 + (CollectionUtils.isEmpty(transforms) ? 0 : transforms.size()));
    stepConfigs.addAll(sources);
    if (CollectionUtils.isNotEmpty(transforms)) {
      stepConfigs.addAll(transforms);
    }
    stepConfigs.add(sink);
    return stepConfigs;
  }

  private void loadJobConfig(String json) {
    JsonNode jsonNode =
        JsonUtils.toJsonNode(json)
            .orElseThrow(
                () ->
                    new DataWorkflowException("job config json parse error. json: [" + json + "]"));
    String jobName = jsonNode.path("jobName").textValue();
    String jobMode = jsonNode.path("jobMode").textValue();
    this.setJobName(jobName);
    this.setJobMode(JobMode.of(jobMode));
    JsonNode env = jsonNode.path("env");
    RuntimeConfig envConfig = new RuntimeConfig();
    if (JsonUtils.isNotNull(env)) {
      JsonNode flushIntervalNode = env.get("flushInterval");
      if (JsonUtils.isNotNull(flushIntervalNode)) {
        long flushInterval = flushIntervalNode.asLong();
        envConfig.setFlushInterval(flushInterval);
      }
      JsonNode metricsIntervalNode = env.get("metricsInterval");
      if (JsonUtils.isNotNull(metricsIntervalNode)) {
        long metricsInterval = metricsIntervalNode.asLong();
        envConfig.setMetricsInterval(metricsInterval);
      }
      JsonNode batchSizeNode = env.get("batchSize");
      if (JsonUtils.isNotNull(batchSizeNode)) {
        long batchSize = batchSizeNode.asLong();
        envConfig.setBatchSize(batchSize);
      }

      {
        JsonNode runtimeconfigJsonNode = jsonNode.path("runtimeConfig");
        Map<String, Object> runtimeConfig = new HashMap<>();
        if (JsonUtils.isNotNull(runtimeconfigJsonNode)) {
          runtimeConfig =
              JsonUtils.fromJson(
                      runtimeconfigJsonNode.asText(), new TypeReference<Map<String, Object>>() {})
                  .orElseThrow(
                      () ->
                          new DataWorkflowException(
                              "runtime config parse error. json: ["
                                  + runtimeconfigJsonNode.asText()
                                  + "]"));
        }
        envConfig.setExtra(runtimeConfig);
      }
    }
    this.setRuntimeConfig(envConfig);

    // sources node
    {
      JsonNode sources = jsonNode.path("sources");
      if (JsonUtils.isNull(sources)) {
        throw new DataWorkflowException("sources can not be null, json: [" + json + "]");
      }
      if (!sources.isArray()) {
        throw new DataWorkflowException(
            "sources json node must array node, json: [" + sources + "]");
      }
      ArrayNode sourceArrayNode = (ArrayNode) sources;
      List<SourceConfig<?>> sourceConfigs = new ArrayList<>(sourceArrayNode.size());
      for (int i = 0; i < sourceArrayNode.size(); i++) {
        JsonNode sourceConfigNode = sourceArrayNode.get(i);
        String type = sourceConfigNode.path("type").textValue();
        SourceProvider<? extends SourceConfig<?>, ?> sourceProvider =
            PluginRegistry.getSourceProvider(type)
                .orElseThrow(
                    () ->
                        new DataWorkflowException(
                            String.format("source provider [%s] not found", type)));
        SourceConfig<?> sourceConfig = sourceProvider.getSourceConfig();
        sourceConfigs.add(sourceConfig.toSourceConfig(sourceConfigNode.toString()));
      }
      this.setSources(sourceConfigs);
    }

    // transformNodes
    {
      JsonNode transforms = jsonNode.path("transforms");
      if (JsonUtils.isNotNull(transforms)) {
        if (!transforms.isArray()) {
          throw new DataWorkflowException(
              "transforms json node must array node, json: [" + transforms + "]");
        }

        ArrayNode transformArrayNode = (ArrayNode) transforms;
        List<TransformConfig<?>> transformConfigs = new ArrayList<>(transformArrayNode.size());
        for (int i = 0; i < transformArrayNode.size(); i++) {
          JsonNode transformJsoNode = transformArrayNode.get(i);
          String type = transformJsoNode.path("type").textValue();
          TransformProvider<?> transformProvider =
              PluginRegistry.getTransformProvider(type)
                  .orElseThrow(
                      () ->
                          new DataWorkflowException(
                              String.format("transform provider [%s] not found", type)));
          TransformConfig<?> transformConfig = transformProvider.getTransformConfig();
          transformConfigs.add(transformConfig.toTransformConfig(transformJsoNode.toString()));
        }
        this.setTransforms(transformConfigs);
      }
    }

    // sinks node
    {
      JsonNode sinkNode = jsonNode.path("sink");
      if (JsonUtils.isNull(sinkNode)) {
        throw new DataWorkflowException("sinks can not be null, json: [" + json + "]");
      }
      if (sinkNode.isArray()) {
        throw new DataWorkflowException(
            "sinks json node is not array node, json: [" + sinkNode + "]");
      }
      String type = sinkNode.path("type").textValue();
      SinkProvider<?> sinkProvider =
          PluginRegistry.getSinkProvider(type)
              .orElseThrow(
                  () ->
                      new DataWorkflowException(
                          String.format("sink provider [%s] not found", type)));
      SinkConfig<?> sinkConfig = sinkProvider.getSinkConfig();
      this.setSink(sinkConfig.toSinkConfig(sinkNode.toString()));
    }
  }

  public static JobConfigBuilder builder() {
    return new JobConfigBuilder();
  }

  public static class JobConfigBuilder {
    private String jobName;
    private JobMode jobMode = JobMode.BATCH;
    private RuntimeConfig runtimeConfig;
    private List<SourceConfig<?>> sources;
    private List<TransformConfig<?>> transforms;
    private SinkConfig<?> sink;

    public JobConfigBuilder jobName(String jobName) {
      this.jobName = jobName;
      return this;
    }

    public JobConfigBuilder jobMode(JobMode jobMode) {
      if (jobMode == null) {
        this.jobMode = JobMode.BATCH;
        return this;
      }
      this.jobMode = jobMode;
      return this;
    }

    public JobConfigBuilder runtimeConfig(RuntimeConfig runtimeConfig) {
      this.runtimeConfig = runtimeConfig;
      return this;
    }

    public JobConfigBuilder sources(List<SourceConfig<?>> sources) {
      this.sources = sources;
      return this;
    }

    public JobConfigBuilder transforms(List<TransformConfig<?>> transforms) {
      this.transforms = transforms;
      return this;
    }

    public JobConfigBuilder sink(SinkConfig<?> sink) {
      this.sink = sink;
      return this;
    }

    public JobConfig build() {
      JobConfig jobConfig = new JobConfig();
      jobConfig.jobName = this.jobName;
      jobConfig.jobMode = this.jobMode;
      jobConfig.runtimeConfig = this.runtimeConfig;
      jobConfig.sources = this.sources;
      jobConfig.transforms = this.transforms;
      jobConfig.sink = this.sink;
      return jobConfig;
    }
  }
}
