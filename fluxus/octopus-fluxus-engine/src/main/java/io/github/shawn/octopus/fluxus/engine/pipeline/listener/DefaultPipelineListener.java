package io.github.shawn.octopus.fluxus.engine.pipeline.listener;

import io.github.shawn.octopus.fluxus.api.exception.PipelineException;
import io.github.shawn.octopus.fluxus.engine.pipeline.context.JobContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultPipelineListener implements PipelineListener {

  private final JobContext jobContext;

  public DefaultPipelineListener(JobContext jobContext) {
    this.jobContext = jobContext;
  }

  @Override
  public void onPrepare() throws PipelineException {
    jobContext.setPrepare();
  }

  @Override
  public void onStart() throws PipelineException {
    jobContext.setRunning();
  }

  @Override
  public void onShutdown() throws PipelineException {
    Throwable throwable = jobContext.getThrowable();
    if (throwable != null) {
      log.error("pipeline-{} run error...", jobContext.getJobConfig().getJobId(), throwable);
    }
  }
}
