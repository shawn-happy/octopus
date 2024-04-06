package io.github.shawn.octopus.fluxus.engine.pipeline.config;

public enum PipelineStatus {
  CREATING,
  PREPARE,
  RUNNING,
  FAILED,
  SUCCESS,
  CANCELED,
  ;

  public boolean isStarted() {
    return CREATING.equals(this) || PREPARE.equals(this) || RUNNING.equals(this);
  }

  public boolean isQuit() {
    return FAILED.equals(this) || SUCCESS.equals(this) || CANCELED.equals(this);
  }
}
