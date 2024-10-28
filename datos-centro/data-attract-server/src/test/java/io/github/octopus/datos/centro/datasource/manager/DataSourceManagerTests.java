package io.github.octopus.datos.centro.datasource.manager;

import io.github.octopus.datos.centro.datasource.manager.jdbc.JdbcDataSourceManager;
import io.github.octopus.datos.centro.model.bo.datasource.jdbc.JdbcDataSourceConfig;
import org.junit.jupiter.api.Test;

public class DataSourceManagerTests {

  @Test
  public void mySQLConnect() {
    JdbcDataSourceConfig dataSourceConfig =
        JdbcDataSourceConfig.builder()
            .url("jdbc:mysql://192.168.1.6:3306/data-center?serverTimezone=GMT%2B8")
            .username("root")
            .password("123456")
            .driverClassName("com.mysql.cj.jdbc.Driver")
            .database("data-center")
            .build();
    JdbcDataSourceManager jdbcDataSourceManager = new JdbcDataSourceManager();
    jdbcDataSourceManager.connect(dataSourceConfig);
  }
}
