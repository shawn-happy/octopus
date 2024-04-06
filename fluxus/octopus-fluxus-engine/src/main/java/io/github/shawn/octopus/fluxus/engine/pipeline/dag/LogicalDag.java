package io.github.shawn.octopus.fluxus.engine.pipeline.dag;

import io.github.shawn.octopus.fluxus.api.common.IdGenerator;
import io.github.shawn.octopus.fluxus.api.config.SinkConfig;
import io.github.shawn.octopus.fluxus.api.config.SourceConfig;
import io.github.shawn.octopus.fluxus.api.config.StepConfig;
import io.github.shawn.octopus.fluxus.api.config.TransformConfig;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.engine.pipeline.config.JobConfig;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;

public class LogicalDag {
  @Getter private final JobConfig jobConfig;
  @Getter private final List<LogicalEdge> edges = new LinkedList<>();
  @Getter private final Map<String, LogicalVertex> nodes = new LinkedHashMap<>();
  private final Map<String, StepConfig> stepCaches;
  // output与step name的映射关系
  private final Map<String, String> tables = new LinkedHashMap<>();
  private final IdGenerator idGenerator = new IdGenerator();

  public LogicalDag(JobConfig jobConfig) {
    this.jobConfig = jobConfig;
    List<StepConfig> steps = jobConfig.getSteps();
    stepCaches = new LinkedHashMap<>(steps.size());
    steps.forEach(step -> stepCaches.put(step.getName(), step));
    initResultTableMapping();
    createLogicalEdgeAndVertex();
    tables.clear();
    stepCaches.clear();
  }

  private void initResultTableMapping() {
    List<SourceConfig<?>> sources = jobConfig.getSources();
    List<TransformConfig<?>> transforms = jobConfig.getTransforms();
    sources.forEach(source -> tables.putIfAbsent(source.getOutput(), source.getName()));
    if (CollectionUtils.isNotEmpty(transforms)) {
      transforms.forEach(
          transform -> tables.putIfAbsent(transform.getOutput(), transform.getName()));
    }
  }

  private void createLogicalEdgeAndVertex() {
    List<TransformConfig<?>> transforms = jobConfig.getTransforms();
    SinkConfig<?> sink = jobConfig.getSink();
    if (CollectionUtils.isNotEmpty(transforms)) {
      for (TransformConfig<?> transform : transforms) {
        List<String> inputs = transform.getInputs();
        List<LogicalVertex> transformInputVertex = new ArrayList<>(inputs.size());
        for (String input : inputs) {
          LogicalVertex fromVertex = createLogicalVertex(tables.get(input));
          transformInputVertex.add(fromVertex);
        }
        LogicalVertex toVertex = createLogicalVertex(transform.getName());
        transformInputVertex.forEach(
            inputVertex -> edges.add(new LogicalEdge(inputVertex, toVertex)));
      }
    }

    LogicalVertex fromVertex = createLogicalVertex(tables.get(sink.getInput()));
    LogicalVertex toVertex = createLogicalVertex(sink.getName());
    LogicalEdge logicalEdge = new LogicalEdge(fromVertex, toVertex);
    edges.add(logicalEdge);
  }

  private LogicalVertex createLogicalVertex(String name) {
    StepConfig toStep = stepCaches.get(name);
    if (toStep == null) {
      throw new DataWorkflowException(String.format("dag node [%s] not found", name));
    }
    return nodes.computeIfAbsent(
        toStep.getName(),
        e ->
            LogicalVertex.builder()
                .vertexId(idGenerator.getNextId())
                .name(toStep.getName())
                .stepConfig(toStep)
                .build());
  }
}
