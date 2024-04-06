package io.github.shawn.octopus.fluxus.engine.connector.source.pulsar;

import io.github.shawn.octopus.fluxus.api.config.SourceConfig;
import io.github.shawn.octopus.fluxus.api.connector.Source;
import io.github.shawn.octopus.fluxus.api.converter.RowRecordConverter;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.provider.SourceProvider;
import io.github.shawn.octopus.fluxus.engine.common.Constants;
import io.github.shawn.octopus.fluxus.engine.model.table.SchemaUtils;
import org.apache.pulsar.client.api.Message;

public class PulsarSourceProvider implements SourceProvider<PulsarSourceConfig, Message<byte[]>> {
  @Override
  public String getIdentifier() {
    return Constants.SourceConstants.PULSAR_SOURCE;
  }

  @Override
  public PulsarSourceConfig getSourceConfig() {
    return new PulsarSourceConfig();
  }

  @Override
  public Source<PulsarSourceConfig> getSource(SourceConfig<?> sourceConfig) {
    if (!(sourceConfig instanceof PulsarSourceConfig)) {
      throw new DataWorkflowException("source config type is not pulsar");
    }
    return new PulsarSource((PulsarSourceConfig) sourceConfig);
  }

  @Override
  public RowRecordConverter<Message<byte[]>> getConvertor(SourceConfig<?> sourceConfig) {
    return new PulsarMessageConverter(SchemaUtils.getSchemas(sourceConfig.getColumns()));
  }

  @Override
  public boolean supportedStream() {
    return true;
  }
}
