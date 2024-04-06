package io.github.shawn.octopus.fluxus.engine.connector;

import io.github.shawn.octopus.fluxus.api.config.SourceConfig;
import io.github.shawn.octopus.fluxus.api.connector.Source;
import io.github.shawn.octopus.fluxus.api.converter.RowRecordConverter;
import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.engine.pipeline.context.JobContext;
import io.github.shawn.octopus.fluxus.engine.pipeline.context.JobContextManagement;
import io.github.shawn.octopus.fluxus.engine.pipeline.monitor.PipelineMetricSummary;

public class SourceAdapter<P extends SourceConfig<?>> implements Source<P> {

  private final Source<P> delegate;

  public SourceAdapter(Source<P> source) {
    this.delegate = source;
  }

  @Override
  public void init() throws StepExecutionException {
    delegate.init();
  }

  @Override
  public void dispose() throws StepExecutionException {
    delegate.dispose();
  }

  @Override
  public void begin() throws StepExecutionException {
    delegate.begin();
  }

  @Override
  public RowRecord read() throws StepExecutionException {
    PipelineMetricSummary pipelineMetricSummary =
        JobContextManagement.getJob().getPipelineMetricSummary();
    long startTime = System.nanoTime();
    RowRecord record = delegate.read();
    if (record != null) {
      pipelineMetricSummary.incrSourceCount(record.size());
      pipelineMetricSummary.incrSourceTimer(System.nanoTime() - startTime);
    }
    return record;
  }

  @Override
  public boolean commit() throws StepExecutionException {
    JobContext jobContext = JobContextManagement.getJob();
    PipelineMetricSummary pipelineMetricSummary = jobContext.getPipelineMetricSummary();
    long start = System.nanoTime();
    boolean commit = delegate.commit();
    pipelineMetricSummary.incrSourceFlushTimer(System.nanoTime() - start);
    return commit;
  }

  @Override
  public void abort() throws StepExecutionException {
    delegate.abort();
  }

  @Override
  public <S> void setConverter(RowRecordConverter<S> convertor) {
    delegate.setConverter(convertor);
  }

  @Override
  public P getSourceConfig() {
    return delegate.getSourceConfig();
  }
}
