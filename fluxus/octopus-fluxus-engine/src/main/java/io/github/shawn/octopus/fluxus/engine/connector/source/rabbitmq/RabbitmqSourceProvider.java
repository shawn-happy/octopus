package io.github.shawn.octopus.fluxus.engine.connector.source.rabbitmq;

import io.github.shawn.octopus.fluxus.api.config.SourceConfig;
import io.github.shawn.octopus.fluxus.api.connector.Source;
import io.github.shawn.octopus.fluxus.api.converter.RowRecordConverter;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.provider.SourceProvider;
import io.github.shawn.octopus.fluxus.engine.common.Constants;
import io.github.shawn.octopus.fluxus.engine.model.table.SchemaUtils;

/** @author zuoyl */
public class RabbitmqSourceProvider implements SourceProvider<RabbitmqSourceConfig, byte[]> {

  @Override
  public String getIdentifier() {
    return Constants.SourceConstants.RABBITMQ_SOURCE;
  }

  @Override
  public RabbitmqSourceConfig getSourceConfig() {
    return new RabbitmqSourceConfig();
  }

  @Override
  public Source<RabbitmqSourceConfig> getSource(SourceConfig<?> sourceConfig) {
    if (!(sourceConfig instanceof RabbitmqSourceConfig)) {
      throw new DataWorkflowException("source config type is not rabbitmq");
    }
    return new RabbitmqSource((RabbitmqSourceConfig) sourceConfig);
  }

  @Override
  public RowRecordConverter<byte[]> getConvertor(SourceConfig<?> sourceConfig) {
    return new RabbitmqMessageConverter(SchemaUtils.getSchemas(sourceConfig.getColumns()));
  }
}
