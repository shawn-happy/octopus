package io.github.shawn.octopus.fluxus.engine.connector.sink.console;

import io.github.shawn.octopus.fluxus.api.config.SinkConfig;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.provider.SinkProvider;
import io.github.shawn.octopus.fluxus.engine.common.Constants;

public class ConsoleSinkProvider implements SinkProvider<ConsoleSinkConfig> {
  @Override
  public String getIdentifier() {
    return Constants.SinkConstants.CONSOLE_SINK;
  }

  @Override
  public ConsoleSinkConfig getSinkConfig() {
    return new ConsoleSinkConfig();
  }

  @Override
  public ConsoleSink getSink(SinkConfig<?> sinkConfig) {
    if (!(sinkConfig instanceof ConsoleSinkConfig)) {
      throw new DataWorkflowException("sink config type is not console");
    }
    return new ConsoleSink((ConsoleSinkConfig) sinkConfig);
  }
}
