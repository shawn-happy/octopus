package io.github.shawn.octopus.fluxus.engine.connector.sink.pulsar;

import io.github.shawn.octopus.fluxus.api.config.SinkConfig;
import io.github.shawn.octopus.fluxus.api.connector.Sink;
import io.github.shawn.octopus.fluxus.api.provider.SinkProvider;
import io.github.shawn.octopus.fluxus.engine.common.Constants;

public class PulsarSinkProvider implements SinkProvider<PulsarSinkConfig> {
  @Override
  public String getIdentifier() {
    return Constants.SinkConstants.PULSAR;
  }

  @Override
  public PulsarSinkConfig getSinkConfig() {
    return new PulsarSinkConfig();
  }

  @Override
  public Sink<PulsarSinkConfig> getSink(SinkConfig<?> sinkConfig) {
    return new PulsarSink((PulsarSinkConfig) sinkConfig);
  }
}
