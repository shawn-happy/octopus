package io.github.octopus.datos.centro.datasource.manager.jdbc;

import io.github.octopus.datos.centro.common.exception.DataCenterServiceException;
import io.github.octopus.datos.centro.datasource.manager.DataSourceManager;
import io.github.octopus.datos.centro.model.bo.datasource.jdbc.JdbcDataSourceConfig;
import io.github.octopus.datos.centro.util.JdbcUtil;
import java.sql.Connection;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcDataSourceManager implements DataSourceManager<JdbcDataSourceConfig> {

  @Override
  public void connect(JdbcDataSourceConfig config) throws DataCenterServiceException {
    if (config == null) {
      throw new DataCenterServiceException(
          "jdbc datasource config is null, please check your config");
    }
    try (Connection ignore =
        JdbcUtil.getConnection(config.getUrl(), config.getUsername(), config.getPassword())) {
      log.info("jdbc datasource connect success.");
    } catch (Exception e) {
      log.error(
          "jdbc datasource connect error. url: {}, username: {}, password: {}",
          config.getUrl(),
          config.getUsername(),
          config.getPassword(),
          e);
      throw new DataCenterServiceException(
          String.format(
              "jdbc datasource connect error. url: %s, username: %s, password: %s",
              config.getUrl(), config.getUsername(), config.getPassword()),
          e);
    }
  }
}
