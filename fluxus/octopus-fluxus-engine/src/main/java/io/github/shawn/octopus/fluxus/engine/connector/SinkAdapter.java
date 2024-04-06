package io.github.shawn.octopus.fluxus.engine.connector;

import io.github.shawn.octopus.fluxus.api.config.SinkConfig;
import io.github.shawn.octopus.fluxus.api.connector.Sink;
import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.engine.pipeline.context.JobContext;
import io.github.shawn.octopus.fluxus.engine.pipeline.context.JobContextManagement;
import io.github.shawn.octopus.fluxus.engine.pipeline.monitor.PipelineMetricSummary;

public class SinkAdapter<P extends SinkConfig<?>> implements Sink<P> {

  private final Sink<P> delegate;

  public SinkAdapter(Sink<P> sink) {
    this.delegate = sink;
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
  public void write(RowRecord source) throws StepExecutionException {
    JobContext jobContext = JobContextManagement.getJob();
    PipelineMetricSummary pipelineMetricSummary = jobContext.getPipelineMetricSummary();
    long startTime = System.nanoTime();
    delegate.write(source);
    pipelineMetricSummary.incrSinkTimer(System.nanoTime() - startTime);
    pipelineMetricSummary.incrSinkCount(source.size());
  }

  @Override
  public P getSinkConfig() {
    return delegate.getSinkConfig();
  }

  @Override
  public boolean commit() throws StepExecutionException {
    JobContext jobContext = JobContextManagement.getJob();
    PipelineMetricSummary pipelineMetricSummary = jobContext.getPipelineMetricSummary();
    long startTime = System.nanoTime();
    boolean commit = delegate.commit();
    pipelineMetricSummary.incrSinkFlushTimer(System.nanoTime() - startTime);
    return commit;
  }

  @Override
  public void abort() throws StepExecutionException {
    delegate.abort();
  }
}
