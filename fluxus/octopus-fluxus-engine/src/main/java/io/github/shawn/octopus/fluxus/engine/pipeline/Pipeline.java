package io.github.shawn.octopus.fluxus.engine.pipeline;

import io.github.shawn.octopus.fluxus.api.exception.PipelineException;
import io.github.shawn.octopus.fluxus.engine.pipeline.config.JobConfig;
import io.github.shawn.octopus.fluxus.engine.pipeline.config.PipelineStatus;
import io.github.shawn.octopus.fluxus.engine.pipeline.context.JobContext;

public interface Pipeline extends Runnable {
  JobConfig getJobConfig();

  JobContext getJobContext();

  void prepare() throws PipelineException;

  void cancel() throws PipelineException;

  void shutdown() throws PipelineException;

  PipelineStatus getStatus();
}
