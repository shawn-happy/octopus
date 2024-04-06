package io.github.shawn.octopus.fluxus.engine.connector.source.jdbc;

import io.github.shawn.octopus.fluxus.api.config.SourceConfig;
import io.github.shawn.octopus.fluxus.api.converter.RowRecordConverter;
import io.github.shawn.octopus.fluxus.api.provider.SourceProvider;
import io.github.shawn.octopus.fluxus.engine.common.Constants;
import io.github.shawn.octopus.fluxus.engine.model.table.SchemaUtils;
import java.sql.ResultSet;

public class JdbcSourceProvider implements SourceProvider<JdbcSourceConfig, ResultSet> {
  @Override
  public String getIdentifier() {
    return Constants.SourceConstants.JDBC;
  }

  @Override
  public JdbcSourceConfig getSourceConfig() {
    return new JdbcSourceConfig();
  }

  @Override
  public JdbcSource getSource(SourceConfig<?> sourceConfig) {
    return new JdbcSource((JdbcSourceConfig) sourceConfig);
  }

  @Override
  public RowRecordConverter<ResultSet> getConvertor(SourceConfig<?> sourceConfig) {
    return new ResultSetConverter(SchemaUtils.getSchemas(sourceConfig.getColumns()));
  }
}
