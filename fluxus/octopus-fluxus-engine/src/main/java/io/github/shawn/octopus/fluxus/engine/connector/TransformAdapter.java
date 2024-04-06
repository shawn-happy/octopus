package io.github.shawn.octopus.fluxus.engine.connector;

import io.github.shawn.octopus.fluxus.api.config.TransformConfig;
import io.github.shawn.octopus.fluxus.api.connector.Transform;
import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.engine.pipeline.context.JobContext;
import io.github.shawn.octopus.fluxus.engine.pipeline.context.JobContextManagement;
import io.github.shawn.octopus.fluxus.engine.pipeline.monitor.PipelineMetricSummary;

public class TransformAdapter<P extends TransformConfig<?>> implements Transform<P> {

  private final Transform<P> delegate;

  public TransformAdapter(Transform<P> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void init() throws StepExecutionException {
    delegate.init();
  }

  @Override
  public void begin() throws StepExecutionException {
    delegate.begin();
  }

  @Override
  public void dispose() throws StepExecutionException {
    delegate.dispose();
  }

  @Override
  public RowRecord transform(RowRecord source) throws StepExecutionException {
    JobContext jobContext = JobContextManagement.getJob();
    PipelineMetricSummary pipelineMetricSummary = jobContext.getPipelineMetricSummary();
    long startTime = System.nanoTime();
    RowRecord transform = delegate.transform(source);
    pipelineMetricSummary.incrTransformTimer(
        delegate.getTransformConfig().getId(), System.nanoTime() - startTime);
    pipelineMetricSummary.incrTransformCount(delegate.getTransformConfig().getId(), source.size());
    return transform;
  }

  @Override
  public P getTransformConfig() {
    return delegate.getTransformConfig();
  }
}
