package io.github.shawn.octopus.fluxus.engine.pipeline;

import io.github.shawn.octopus.fluxus.api.config.SinkConfig;
import io.github.shawn.octopus.fluxus.api.config.SourceConfig;
import io.github.shawn.octopus.fluxus.api.config.StepConfig;
import io.github.shawn.octopus.fluxus.api.config.TransformConfig;
import io.github.shawn.octopus.fluxus.api.connector.Sink;
import io.github.shawn.octopus.fluxus.api.connector.Source;
import io.github.shawn.octopus.fluxus.api.connector.Transform;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.provider.PluginRegistry;
import io.github.shawn.octopus.fluxus.api.provider.SinkProvider;
import io.github.shawn.octopus.fluxus.api.provider.SourceProvider;
import io.github.shawn.octopus.fluxus.api.provider.TransformProvider;
import io.github.shawn.octopus.fluxus.engine.pipeline.config.JobConfig;
import io.github.shawn.octopus.fluxus.engine.pipeline.dag.LogicalDag;
import io.github.shawn.octopus.fluxus.engine.pipeline.dag.LogicalEdge;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.jetbrains.annotations.NotNull;

public class PipelineGenerator {

  private final LogicalDag logicalDag;
  private Source<?> source;
  private Sink<?> sink;
  private Map<String, Transform<?>> transforms = new LinkedHashMap<>();

  public PipelineGenerator(JobConfig jobConfig) {
    this(new LogicalDag(jobConfig));
  }

  public PipelineGenerator(LogicalDag logicalDag) {
    this.logicalDag = logicalDag;
    checkConfig();
  }

  public Pipeline generate() {
    ExecutionPlan executionPlan = generateExecutePlan(logicalDag.getEdges());
    return new SingletonSourcePipeline(executionPlan);
  }

  private ExecutionPlan generateExecutePlan(List<LogicalEdge> edges) {
    edges.sort(
        (o1, o2) -> {
          if (o1.getFromVertex().getVertexId() != o2.getFromVertex().getVertexId()) {
            return o1.getFromVertex().getVertexId() > o2.getFromVertex().getVertexId() ? 1 : -1;
          }
          if (o1.getToVertex().getVertexId() != o2.getToVertex().getVertexId()) {
            return o1.getToVertex().getVertexId() > o2.getToVertex().getVertexId() ? 1 : -1;
          }
          return 0;
        });

    for (LogicalEdge edge : edges) {
      StepConfig fromStep = edge.getFromVertex().getStepConfig();
      StepConfig toStep = edge.getToVertex().getStepConfig();
      addStep(fromStep);
      addStep(toStep);
    }
    return new ExecutionPlan(
        logicalDag.getJobConfig(),
        source,
        MapUtils.isEmpty(transforms) ? null : new ArrayList<>(transforms.values()),
        sink);
  }

  private void addStep(StepConfig stepConfig) {
    if (stepConfig == null) {
      return;
    }
    if (stepConfig.isSource()) {
      addSource(stepConfig);
    } else if (stepConfig.isSink()) {
      addSink(stepConfig);
    } else if (stepConfig.isTransform()) {
      addTransform(stepConfig);
    }
  }

  private void addSource(@NotNull StepConfig stepConfig) {
    if (stepConfig.isSource()) {
      SourceProvider<? extends SourceConfig<?>, ?> sourceProvider =
          PluginRegistry.getSourceProvider(stepConfig.getIdentifier())
              .orElseThrow(
                  () ->
                      new DataWorkflowException(
                          String.format(
                              "the source plugin [%s] not found", stepConfig.getIdentifier())));
      source = sourceProvider.getSource((SourceConfig<?>) stepConfig);
      source.setConverter(sourceProvider.getConvertor((SourceConfig<?>) stepConfig));
    }
  }

  private void addSink(@NotNull StepConfig stepConfig) {
    if (stepConfig.isSink()) {
      SinkProvider<? extends SinkConfig<?>> sinkProvider =
          PluginRegistry.getSinkProvider(stepConfig.getIdentifier())
              .orElseThrow(
                  () ->
                      new DataWorkflowException(
                          String.format(
                              "the sink plugin [%s] not found", stepConfig.getIdentifier())));
      sink = sinkProvider.getSink((SinkConfig<?>) stepConfig);
    }
  }

  private void addTransform(@NotNull StepConfig stepConfig) {
    if (MapUtils.isEmpty(transforms)) {
      transforms = new LinkedHashMap<>();
    }
    if (stepConfig.isTransform()) {
      TransformProvider<? extends TransformConfig<?>> transformProvider =
          PluginRegistry.getTransformProvider(stepConfig.getIdentifier())
              .orElseThrow(
                  () ->
                      new DataWorkflowException(
                          String.format(
                              "the transform plugin [%s] not found", stepConfig.getIdentifier())));
      transforms.putIfAbsent(
          stepConfig.getName(), transformProvider.getTransform((TransformConfig<?>) stepConfig));
    }
  }

  private void checkConfig() {
    JobConfig jobConfig = logicalDag.getJobConfig();
    List<SourceConfig<?>> sourceConfigs = jobConfig.getSources();
    SinkConfig<?> sinkConfig = jobConfig.getSink();
    if (CollectionUtils.isEmpty(sourceConfigs) || Objects.isNull(sinkConfig)) {
      throw new DataWorkflowException("source or sink can't be null");
    }
    // 未来再考虑支持多个source
    if (sourceConfigs.size() > 1) {
      throw new DataWorkflowException("only supported singleton source");
    }
  }
}
