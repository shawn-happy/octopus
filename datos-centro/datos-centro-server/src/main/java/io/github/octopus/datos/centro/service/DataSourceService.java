package io.github.octopus.datos.centro.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import io.github.octopus.datos.centro.model.bo.datasource.DataSource;
import io.github.octopus.datos.centro.model.bo.datasource.DataSourceConfig;
import io.github.octopus.datos.centro.model.bo.datasource.DataSourceType;
import io.github.octopus.datos.centro.model.bo.form.FormStructure;
import io.github.octopus.datos.centro.model.response.datasource.CheckResult;
import java.time.LocalDateTime;

public interface DataSourceService {

  <T extends DataSourceConfig> CheckResult checkDataSourceConnection(
      DataSourceType type, T dataSourceConfig);

  <T extends DataSourceConfig> DataSource crateDataSource(
      DataSourceType type, String name, String description, T dataSourceConfig);

  <T extends DataSourceConfig> boolean updateDataSource(
      long datasourceId, String datasourceName, String description, T config);

  int deleteDataSource(long id);

  DataSource queryDataSourceById(long id);

  DataSource queryDataSourceByDsName(String dsName);

  IPage<DataSource> queryDataSourceList(
      String datasourceName,
      LocalDateTime createBeginTime,
      LocalDateTime createEndTime,
      Long pageNum,
      Long pageSize);

  FormStructure getDataSourceDynamicForm(String type);
}
