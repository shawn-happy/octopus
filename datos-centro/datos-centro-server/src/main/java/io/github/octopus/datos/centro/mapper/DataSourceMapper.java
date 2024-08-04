package io.github.octopus.datos.centro.mapper;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.github.octopus.datos.centro.common.exception.DataCenterServiceException;
import io.github.octopus.datos.centro.common.utils.JsonUtil;
import io.github.octopus.datos.centro.entity.DataSourceEntity;
import io.github.octopus.datos.centro.model.bo.datasource.DataSource;
import io.github.octopus.datos.centro.model.bo.datasource.DataSourceConfig;
import io.github.octopus.datos.centro.model.response.datasource.DataSourceDetailsVO;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;

public class DataSourceMapper {

  public static DataSourceEntity toDsEntity(DataSource ds) {
    return Optional.ofNullable(ds)
        .map(
            datasource -> {
              DataSourceEntity dataSourceEntity = new DataSourceEntity();
              dataSourceEntity.setName(datasource.getName());
              dataSourceEntity.setType(datasource.getType());
              dataSourceEntity.setDescription(dataSourceEntity.getDescription());
              dataSourceEntity.setConfig(null);
              return dataSourceEntity;
            })
        .orElse(null);
  }

  public static DataSource fromDataSource(DataSourceEntity entity) {
    return Optional.ofNullable(entity)
        .map(
            datasource -> {
              try {
                return DataSource.builder()
                    .name(datasource.getName())
                    .id(datasource.getId())
                    .type(datasource.getType())
                    .description(datasource.getDescription())
                    .config(JsonUtil.fromJson(datasource.getConfig(), DataSourceConfig.class))
                    .createTime(datasource.getCreateTime())
                    .creator(datasource.getCreator())
                    .updater(datasource.getUpdater())
                    .updateTime(datasource.getUpdateTime())
                    .build();
              } catch (JsonProcessingException e) {
                throw new DataCenterServiceException("datasource config json parse error", e);
              }
            })
        .orElse(null);
  }

  public static IPage<DataSource> dataSourceEntityPageMapper(Page<DataSourceEntity> entityPage) {
    if (entityPage == null) {
      return null;
    }
    IPage<DataSource> dsPage = new Page<>();
    dsPage.setCurrent(entityPage.getCurrent());
    dsPage.setTotal(entityPage.getTotal());
    dsPage.setPages(entityPage.getPages());
    dsPage.setSize(entityPage.getSize());
    if (CollectionUtils.isNotEmpty(entityPage.getRecords())) {
      List<DataSource> dsList =
          entityPage
              .getRecords()
              .stream()
              .map(DataSourceMapper::fromDataSource)
              .collect(Collectors.toList());
      dsPage.setRecords(dsList);
    }
    return dsPage;
  }

  public static IPage<DataSourceDetailsVO> dataSourceDetailsVOPageMapper(
      IPage<DataSource> datasourcePage) {
    if (datasourcePage == null) {
      return null;
    }
    IPage<DataSourceDetailsVO> dsPage = new Page<>();
    dsPage.setCurrent(datasourcePage.getCurrent());
    dsPage.setTotal(datasourcePage.getTotal());
    dsPage.setPages(datasourcePage.getPages());
    dsPage.setSize(datasourcePage.getSize());
    if (CollectionUtils.isNotEmpty(datasourcePage.getRecords())) {
      List<DataSourceDetailsVO> dsList =
          datasourcePage
              .getRecords()
              .stream()
              .map(DataSourceMapper::toDataSourceDetailsVO)
              .collect(Collectors.toList());
      dsPage.setRecords(dsList);
    }
    return dsPage;
  }

  public static DataSourceDetailsVO toDataSourceDetailsVO(DataSource dataSource) {
    return Optional.ofNullable(dataSource)
        .map(
            ds ->
                DataSourceDetailsVO.builder()
                    .id(ds.getId())
                    .name(ds.getName())
                    .type(ds.getType())
                    .config(ds.getConfig())
                    .description(ds.getDescription())
                    .createUser(ds.getCreator())
                    .createTime(ds.getCreateTime())
                    .updateUser(ds.getUpdater())
                    .updateTime(ds.getUpdateTime())
                    .build())
        .orElse(null);
  }

  public static DataSourceDetailsVO toDsDetailsVO(DataSource ds) {
    return Optional.ofNullable(ds)
        .map(
            dataSource ->
                DataSourceDetailsVO.builder()
                    .id(dataSource.getId())
                    .type(dataSource.getType())
                    .name(dataSource.getName())
                    .description(dataSource.getDescription())
                    .config(dataSource.getConfig())
                    .createUser(dataSource.getCreator())
                    .createTime(dataSource.getCreateTime())
                    .updateTime(dataSource.getUpdateTime())
                    .updateUser(dataSource.getUpdater())
                    .build())
        .orElse(null);
  }
}
