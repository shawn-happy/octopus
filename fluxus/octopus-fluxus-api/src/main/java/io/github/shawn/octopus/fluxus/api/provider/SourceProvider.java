package io.github.shawn.octopus.fluxus.api.provider;

import io.github.shawn.octopus.fluxus.api.config.SourceConfig;
import io.github.shawn.octopus.fluxus.api.connector.Source;
import io.github.shawn.octopus.fluxus.api.converter.RowRecordConverter;

public interface SourceProvider<P extends SourceConfig<?>, S> {
  String getIdentifier();

  P getSourceConfig();

  Source<P> getSource(SourceConfig<?> sourceConfig);

  RowRecordConverter<S> getConvertor(SourceConfig<?> sourceConfig);

  default boolean supportedStream() {
    return false;
  }
}
