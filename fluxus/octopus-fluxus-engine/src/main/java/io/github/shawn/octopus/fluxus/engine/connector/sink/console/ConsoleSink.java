package io.github.shawn.octopus.fluxus.engine.connector.sink.console;

import io.github.shawn.octopus.fluxus.api.connector.Sink;
import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConsoleSink implements Sink<ConsoleSinkConfig> {

  private final ConsoleSinkConfig config;
  private final ConsoleSinkConfig.ConsoleSinkOptions options;
  private final LongAdder longAdder = new LongAdder();

  public ConsoleSink(ConsoleSinkConfig config) {
    this.config = config;
    this.options = config.getOptions();
  }

  @Override
  public void write(RowRecord source) throws StepExecutionException {
    long line = options.getLine();
    if (line == longAdder.longValue()) {
      return;
    }
    Object[] values;
    while ((values = source.pollNext()) != null) {
      longAdder.increment();
      log.info("{}", Arrays.toString(values));
    }
  }

  @Override
  public ConsoleSinkConfig getSinkConfig() {
    return config;
  }

  @Override
  public boolean commit() throws StepExecutionException {
    return true;
  }

  @Override
  public void abort() throws StepExecutionException {}
}
