package io.github.shawn.octopus.fluxus.engine.connector.source.activemq;

import io.github.shawn.octopus.fluxus.api.config.SourceConfig;
import io.github.shawn.octopus.fluxus.api.connector.Source;
import io.github.shawn.octopus.fluxus.api.converter.RowRecordConverter;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.provider.SourceProvider;
import io.github.shawn.octopus.fluxus.engine.common.Constants;
import io.github.shawn.octopus.fluxus.engine.model.table.SchemaUtils;

/** @author zuoyl */
public class ActivemqSourceProvider implements SourceProvider<ActivemqSourceConfig, String> {

  @Override
  public String getIdentifier() {
    return Constants.SourceConstants.ACTIVEMQ_SOURCE;
  }

  @Override
  public ActivemqSourceConfig getSourceConfig() {
    return new ActivemqSourceConfig();
  }

  @Override
  public Source<ActivemqSourceConfig> getSource(SourceConfig<?> sourceConfig) {
    if (!(sourceConfig instanceof ActivemqSourceConfig)) {
      throw new DataWorkflowException("source config type is not pulsar");
    }
    return new ActivemqSource((ActivemqSourceConfig) sourceConfig);
  }

  @Override
  public RowRecordConverter<String> getConvertor(SourceConfig<?> sourceConfig) {
    return new ActivemqMessageConverter(SchemaUtils.getSchemas(sourceConfig.getColumns()));
  }
}
