package io.github.shawn.octopus.fluxus.engine.pipeline.listener;

import io.github.shawn.octopus.fluxus.api.exception.PipelineException;

public interface PipelineListener {

  void onPrepare() throws PipelineException;

  void onStart() throws PipelineException;

  void onShutdown() throws PipelineException;
}
