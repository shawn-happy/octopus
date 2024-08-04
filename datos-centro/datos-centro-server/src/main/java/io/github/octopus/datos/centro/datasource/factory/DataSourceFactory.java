package io.github.octopus.datos.centro.datasource.factory;

import io.github.octopus.datos.centro.datasource.manager.DataSourceManager;
import io.github.octopus.datos.centro.model.bo.datasource.DataSource;
import io.github.octopus.datos.centro.model.bo.datasource.DataSourceConfig;
import io.github.octopus.datos.centro.model.bo.datasource.DataSourceType;
import io.github.octopus.datos.centro.model.bo.form.FormStructure;

public interface DataSourceFactory {

  <T extends DataSourceConfig> DataSource createDataSource(
      DataSourceType dsType, String dataSourceName, String description, T config);

  String factoryIdentifier();

  <T extends DataSourceConfig> DataSourceManager<T> createDataSourceManager();

  FormStructure createDataSourceDynamicForm();
}
