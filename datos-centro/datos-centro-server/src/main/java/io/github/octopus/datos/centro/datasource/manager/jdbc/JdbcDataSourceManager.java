package io.github.octopus.datos.centro.datasource.manager.jdbc;

import io.github.octopus.datos.centro.common.exception.DataCenterServiceException;
import io.github.octopus.datos.centro.common.utils.JsonUtil;
import io.github.octopus.datos.centro.datasource.manager.DataSourceManager;
import io.github.octopus.datos.centro.model.bo.datasource.jdbc.JdbcDataSourceConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class JdbcDataSourceManager implements DataSourceManager<JdbcDataSourceConfig> {

  @Override
  public void connect(JdbcDataSourceConfig config) throws DataCenterServiceException {
    Connection connection = null;
    try{
      if(StringUtils.isNotBlank(config.getUsername())){
        connection = DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword());
      }else{
        connection = DriverManager.getConnection(config.getUrl());
      }

    } catch (SQLException e) {
      log.error("jdbc connect error, config: {}", config);
      throw new DataCenterServiceException(e);
    } finally{
      try{
        if(connection != null){
          connection.close();
        }
      }catch (SQLException e){
        log.error("jdbc connection close error, config: {}", config);
        throw new DataCenterServiceException(e);
      }
    }
  }
}
