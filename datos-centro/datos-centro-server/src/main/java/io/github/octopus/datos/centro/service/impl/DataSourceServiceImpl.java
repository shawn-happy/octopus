package io.github.octopus.datos.centro.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import io.github.octopus.datos.centro.common.exception.DataCenterServiceException;
import io.github.octopus.datos.centro.dao.DataSourceDao;
import io.github.octopus.datos.centro.datasource.factory.DataSourceFactory;
import io.github.octopus.datos.centro.datasource.factory.DataSourceFactoryProvider;
import io.github.octopus.datos.centro.entity.DataSourceEntity;
import io.github.octopus.datos.centro.mapper.DataSourceMapper;
import io.github.octopus.datos.centro.model.bo.datasource.DataSource;
import io.github.octopus.datos.centro.model.bo.datasource.DataSourceConfig;
import io.github.octopus.datos.centro.model.bo.datasource.DataSourceType;
import io.github.octopus.datos.centro.service.DataSourceService;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;

@Service
public class DataSourceServiceImpl implements DataSourceService {

  private final DataSourceDao dataSourceDao;

  public DataSourceServiceImpl(DataSourceDao dataSourceDao) {
    this.dataSourceDao = dataSourceDao;
  }

  @Override
  public <T extends DataSourceConfig> DataSource crateDataSource(DataSourceType type, String name,
      String description, T dataSourceConfig) {
    if(checkUniqueDataSourceName(name, null)){
      throw new DataCenterServiceException(
          String.format("datasource [%s] already exists", name));
    }
    DataSourceFactory dataSourceFactory =
        DataSourceFactoryProvider.getDataSourceFactory(type);
    DataSource dataSource =
        dataSourceFactory.createDataSource(type, name, description, dataSourceConfig);
    DataSourceEntity dataSourceEntity = DataSourceMapper.toDsEntity(dataSource);
    dataSourceDao.insert(dataSourceEntity);
    return dataSource;
  }

  private boolean checkUniqueDataSourceName(String datasourceName, Long id) {
    QueryWrapper<DataSourceEntity> condition = new QueryWrapper<>();
    condition.eq("name", datasourceName);
    if (id != null) {
      condition.ne("id", id);
    }
    return CollectionUtils.isNotEmpty(dataSourceDao.selectList(condition));
  }
}
