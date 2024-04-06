package io.github.shawn.octopus.fluxus.api.connector;

import io.github.shawn.octopus.fluxus.api.config.SinkConfig;
import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;

public interface Sink<P extends SinkConfig<?>> extends LifeCycle {

  void write(RowRecord source) throws StepExecutionException;

  P getSinkConfig();

  boolean commit() throws StepExecutionException;

  void abort() throws StepExecutionException;
}
