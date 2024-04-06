package io.github.shawn.octopus.fluxus.engine.connector.sink.file;

import io.github.shawn.octopus.fluxus.api.config.SinkConfig;
import io.github.shawn.octopus.fluxus.api.connector.Sink;
import io.github.shawn.octopus.fluxus.api.provider.SinkProvider;
import io.github.shawn.octopus.fluxus.engine.common.Constants;

public class FileSinkProvider implements SinkProvider<FileSinkConfig> {
  @Override
  public String getIdentifier() {
    return Constants.SinkConstants.FILE;
  }

  @Override
  public FileSinkConfig getSinkConfig() {
    return new FileSinkConfig();
  }

  @Override
  public Sink<FileSinkConfig> getSink(SinkConfig<?> sinkConfig) {
    return new FileSink((FileSinkConfig) sinkConfig);
  }
}
