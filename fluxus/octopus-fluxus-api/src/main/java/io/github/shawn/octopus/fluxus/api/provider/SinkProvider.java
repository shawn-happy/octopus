package io.github.shawn.octopus.fluxus.api.provider;

import io.github.shawn.octopus.fluxus.api.config.SinkConfig;
import io.github.shawn.octopus.fluxus.api.connector.Sink;

public interface SinkProvider<P extends SinkConfig<?>> {
  String getIdentifier();

  P getSinkConfig();

  Sink<P> getSink(SinkConfig<?> sinkConfig);
}
