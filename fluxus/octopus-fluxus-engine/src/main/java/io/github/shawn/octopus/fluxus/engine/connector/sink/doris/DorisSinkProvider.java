package io.github.shawn.octopus.fluxus.engine.connector.sink.doris;

import static io.github.shawn.octopus.fluxus.engine.common.Constants.SinkConstants.DORIS_SINK;

import io.github.shawn.octopus.fluxus.api.config.SinkConfig;
import io.github.shawn.octopus.fluxus.api.provider.SinkProvider;

public class DorisSinkProvider implements SinkProvider<DorisSinkConfig> {
  @Override
  public String getIdentifier() {
    return DORIS_SINK;
  }

  @Override
  public DorisSinkConfig getSinkConfig() {
    return new DorisSinkConfig();
  }

  @Override
  public DorisSink getSink(SinkConfig<?> sinkConfig) {
    return new DorisSink((DorisSinkConfig) sinkConfig);
  }
}
