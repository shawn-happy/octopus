package io.github.octopus.datos.centro.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import io.github.octopus.datos.centro.common.exception.DataCenterServiceException;
import io.github.octopus.datos.centro.common.utils.JsonUtil;
import io.github.octopus.datos.centro.dao.DataSourceDao;
import io.github.octopus.datos.centro.datasource.factory.DataSourceFactory;
import io.github.octopus.datos.centro.datasource.factory.DataSourceFactoryProvider;
import io.github.octopus.datos.centro.datasource.manager.DataSourceManager;
import io.github.octopus.datos.centro.entity.DataSourceEntity;
import io.github.octopus.datos.centro.mapper.DataSourceMapper;
import io.github.octopus.datos.centro.model.bo.datasource.DataSource;
import io.github.octopus.datos.centro.model.bo.datasource.DataSourceConfig;
import io.github.octopus.datos.centro.model.bo.datasource.DataSourceType;
import io.github.octopus.datos.centro.model.bo.form.FormStructure;
import io.github.octopus.datos.centro.model.response.datasource.CheckResult;
import io.github.octopus.datos.centro.service.DataSourceService;
import java.time.LocalDateTime;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DataSourceServiceImpl implements DataSourceService {

  private static final Logger log = LoggerFactory.getLogger(DataSourceServiceImpl.class);
  private final DataSourceDao dataSourceDao;

  public DataSourceServiceImpl(DataSourceDao dataSourceDao) {
    this.dataSourceDao = dataSourceDao;
  }

  @Override
  public <T extends DataSourceConfig> CheckResult checkDataSourceConnection(
      DataSourceType type, T dataSourceConfig) {
    String identifier = type.identifier();
    DataSourceFactory dataSourceFactory =
        DataSourceFactoryProvider.getDataSourceFactory(identifier);
    DataSourceManager<T> dataSourceManager = dataSourceFactory.createDataSourceManager();
    try {
      dataSourceManager.connect(dataSourceConfig);
      return CheckResult.builder().success(true).message("connect success.").build();
    } catch (Exception e) {
      log.error("connect error.", e);
      return CheckResult.builder().success(false).message("connect error").build();
    }
  }

  @Override
  public <T extends DataSourceConfig> DataSource crateDataSource(
      DataSourceType type, String name, String description, T dataSourceConfig) {
    if (checkUniqueDataSourceName(name, null)) {
      throw new DataCenterServiceException(String.format("datasource [%s] already exists", name));
    }
    DataSourceFactory dataSourceFactory =
        DataSourceFactoryProvider.getDataSourceFactory(type.identifier());
    DataSource dataSource =
        dataSourceFactory.createDataSource(type, name, description, dataSourceConfig);
    DataSourceEntity dataSourceEntity = DataSourceMapper.toDsEntity(dataSource);
    dataSourceDao.insert(dataSourceEntity);
    return dataSource;
  }

  @Override
  public <T extends DataSourceConfig> boolean updateDataSource(
      long datasourceId, String datasourceName, String description, T config) {
    DataSourceEntity dataSourceEntity = dataSourceDao.selectById(datasourceId);
    if (null == dataSourceEntity) {
      throw new DataCenterServiceException(
          String.format("datasource not found by id [%d]", datasourceId));
    }
    if (StringUtils.isNotBlank(datasourceName)) {
      if (checkUniqueDataSourceName(datasourceName, datasourceId)) {
        throw new DataCenterServiceException(
            String.format("datasource [%s] already exists", datasourceName));
      }
      dataSourceEntity.setName(datasourceName);
    }
    if (StringUtils.isNotBlank(description)) {
      dataSourceEntity.setDescription(description);
    }
    try {
      if (config != null) {
        dataSourceEntity.setConfig(JsonUtil.toJson(config));
      }
    } catch (Exception e) {
      throw new DataCenterServiceException("datasource config to json string error", e);
    }
    return dataSourceDao.updateById(dataSourceEntity) == 1;
  }

  @Override
  public int deleteDataSource(long id) {
    return dataSourceDao.deleteById(id);
  }

  @Override
  public DataSource queryDataSourceById(long id) {
    return DataSourceMapper.fromDataSource(dataSourceDao.selectById(id));
  }

  @Override
  public DataSource queryDataSourceByDsName(String dsName) {
    return DataSourceMapper.fromDataSource(
        dataSourceDao.selectOne(new QueryWrapper<DataSourceEntity>().eq("name", dsName)));
  }

  @Override
  public IPage<DataSource> queryDataSourceList(
      String datasourceName,
      LocalDateTime createBeginTime,
      LocalDateTime createEndTime,
      Long pageNum,
      Long pageSize) {
    QueryWrapper<DataSourceEntity> cond = new QueryWrapper<>();
    if (StringUtils.isNotBlank(datasourceName)) {
      cond.like("name", datasourceName);
    }
    if (createBeginTime != null) {
      cond.ge("create_time", createBeginTime);
    }
    if (createEndTime != null) {
      cond.lt("create_time", createEndTime);
    }
    return DataSourceMapper.dataSourceEntityPageMapper(
        dataSourceDao.selectPage(Page.of(pageNum, pageSize), cond));
  }

  @Override
  public FormStructure getDataSourceDynamicForm(String type) {
    return DataSourceFactoryProvider.getDataSourceFactory(type).createDataSourceDynamicForm();
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
