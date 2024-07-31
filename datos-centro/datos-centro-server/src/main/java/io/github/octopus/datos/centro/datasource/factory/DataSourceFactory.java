package io.github.octopus.datos.centro.datasource.factory;

import io.github.octopus.datos.centro.datasource.manager.DataSourceManager;
import io.github.octopus.datos.centro.model.bo.datasource.DataSource;
import io.github.octopus.datos.centro.model.bo.datasource.DataSourceConfig;
import io.github.octopus.datos.centro.model.bo.datasource.DataSourceType;
import io.github.octopus.datos.centro.model.bo.form.FormStructure;

public interface DataSourceFactory {

  DataSource createDataSource(
      DataSourceType dsType, String dataSourceName, String description, DataSourceConfig config);

  String factoryIdentifier();

  DataSourceManager createDataSourceManager();

  FormStructure createDataSourceDynamicForm();

}
