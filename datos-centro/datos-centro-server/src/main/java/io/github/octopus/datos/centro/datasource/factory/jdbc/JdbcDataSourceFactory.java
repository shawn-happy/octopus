package io.github.octopus.datos.centro.datasource.factory.jdbc;

import io.github.octopus.datos.centro.datasource.dynamicForm.JdbcDataSourceFormOptions;
import io.github.octopus.datos.centro.datasource.factory.DataSourceFactory;
import io.github.octopus.datos.centro.datasource.manager.jdbc.JdbcDataSourceManager;
import io.github.octopus.datos.centro.model.bo.datasource.DataSource;
import io.github.octopus.datos.centro.model.bo.datasource.DataSourceConfig;
import io.github.octopus.datos.centro.model.bo.datasource.DataSourceType;
import io.github.octopus.datos.centro.model.bo.form.FormStructure;
import io.github.octopus.datos.centro.util.CodeGenerateUtils;
import java.time.LocalDateTime;

public abstract class JdbcDataSourceFactory implements DataSourceFactory {

  @Override
  public DataSource createDataSource(
      DataSourceType dsType, String dataSourceName, String description, DataSourceConfig config) {
    LocalDateTime now = LocalDateTime.now();
    return DataSource.builder()
        .code(CodeGenerateUtils.getInstance().genCode(factoryIdentifier()))
        .name(dataSourceName)
        .description(description)
        .config(config)
        .type(dsType)
        .createTime(now)
        .updateTime(now)
        .build();
  }

  @Override
  public JdbcDataSourceManager createDataSourceManager() {
    return new JdbcDataSourceManager();
  }

  @Override
  public FormStructure createDataSourceDynamicForm() {
    return FormStructure.builder()
        .name(factoryIdentifier())
        .options(JdbcDataSourceFormOptions.getFormOptions())
        .build();
  }
}
