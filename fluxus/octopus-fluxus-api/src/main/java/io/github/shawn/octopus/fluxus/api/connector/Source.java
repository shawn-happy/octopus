package io.github.shawn.octopus.fluxus.api.connector;

import io.github.shawn.octopus.fluxus.api.config.SourceConfig;
import io.github.shawn.octopus.fluxus.api.converter.RowRecordConverter;
import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;

public interface Source<P extends SourceConfig<?>> extends LifeCycle {

  RowRecord read() throws StepExecutionException;

  boolean commit() throws StepExecutionException;

  void abort() throws StepExecutionException;

  <S> void setConverter(RowRecordConverter<S> convertor);

  P getSourceConfig();
}
