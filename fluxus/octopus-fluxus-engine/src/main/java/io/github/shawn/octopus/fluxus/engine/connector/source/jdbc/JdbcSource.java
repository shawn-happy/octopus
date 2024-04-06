package io.github.shawn.octopus.fluxus.engine.connector.source.jdbc;

import io.github.shawn.octopus.fluxus.api.connector.Source;
import io.github.shawn.octopus.fluxus.api.converter.RowRecordConverter;
import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.engine.common.jdbc.connection.DefaultJdbcConnectionProvider;
import io.github.shawn.octopus.fluxus.engine.common.jdbc.connection.JdbcConnectionConfig;
import io.github.shawn.octopus.fluxus.engine.common.jdbc.connection.JdbcConnectionProvider;
import io.github.shawn.octopus.fluxus.engine.common.jdbc.dialect.JdbcDialect;
import io.github.shawn.octopus.fluxus.engine.common.jdbc.dialect.JdbcDialectLoader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcSource implements Source<JdbcSourceConfig> {

  private final JdbcSourceConfig config;
  private final JdbcSourceConfig.JdbcSourceOptions options;
  private final JdbcConnectionProvider jdbcConnectionProvider;
  private final JdbcDialect jdbcDialect;
  private ResultSetConverter resultSetConverter;
  private PreparedStatement ps;
  private ResultSet resultSet;

  public JdbcSource(JdbcSourceConfig config) {
    this.config = config;
    this.options = config.getOptions();
    this.jdbcConnectionProvider =
        new DefaultJdbcConnectionProvider(
            JdbcConnectionConfig.builder()
                .url(options.getUrl())
                .driverClass(options.getDriver())
                .username(options.getUsername())
                .password(options.getPassword())
                .autoCommit(false)
                .build());
    this.jdbcDialect = JdbcDialectLoader.load(options.getUrl());
  }

  @Override
  public void init() throws StepExecutionException {
    try {
      log.info("jdbc source {} init beginning...", config.getId());
      Connection connection = jdbcConnectionProvider.getOrEstablishConnection();
      this.ps =
          jdbcDialect.creatPreparedStatement(connection, options.getQuery(), options.getLimit());
      resultSet = ps.executeQuery();
      log.info("jdbc source {} init success...", config.getId());
    } catch (Exception e) {
      log.error("jdbc source {} init error...", config.getId());
      throw new StepExecutionException(e);
    }
  }

  @Override
  public RowRecord read() throws StepExecutionException {
    try {
      if (resultSet.next()) {
        return resultSetConverter.convert(resultSet);
      } else {
        return null;
      }
    } catch (SQLException e) {
      throw new StepExecutionException(e);
    }
  }

  @Override
  public boolean commit() throws StepExecutionException {
    return true;
  }

  @Override
  public void abort() throws StepExecutionException {}

  @Override
  public void dispose() throws StepExecutionException {
    try {
      log.info("jdbc source {} close resources beginning...", config.getId());
      if (resultSet != null) {
        resultSet.close();
      }
      if (ps != null) {
        ps.close();
      }
      jdbcConnectionProvider.closeConnection();
      log.info("jdbc source {} close resources success...", config.getId());
    } catch (Exception e) {
      log.error("jdbc source {} close resources success...", config.getId(), e);
      throw new StepExecutionException(e);
    } finally {
      resultSet = null;
      ps = null;
    }
  }

  @Override
  public <S> void setConverter(RowRecordConverter<S> converter) {
    this.resultSetConverter = (ResultSetConverter) converter;
  }

  @Override
  public JdbcSourceConfig getSourceConfig() {
    return config;
  }
}
