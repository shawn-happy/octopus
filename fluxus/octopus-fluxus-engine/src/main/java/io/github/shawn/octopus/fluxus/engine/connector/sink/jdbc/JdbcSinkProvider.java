package io.github.shawn.octopus.fluxus.engine.connector.sink.jdbc;

import io.github.shawn.octopus.fluxus.api.config.SinkConfig;
import io.github.shawn.octopus.fluxus.api.connector.Sink;
import io.github.shawn.octopus.fluxus.api.provider.SinkProvider;
import io.github.shawn.octopus.fluxus.engine.common.Constants;

public class JdbcSinkProvider implements SinkProvider<JdbcSinkConfig> {
  @Override
  public String getIdentifier() {
    return Constants.SinkConstants.JDBC_SINK;
  }

  @Override
  public JdbcSinkConfig getSinkConfig() {
    return new JdbcSinkConfig();
  }

  @Override
  public Sink<JdbcSinkConfig> getSink(SinkConfig<?> sinkConfig) {
    return new JdbcSink((JdbcSinkConfig) sinkConfig);
  }
}
