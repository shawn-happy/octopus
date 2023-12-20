package com.octopus.actus.connector.jdbc.service.impl;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.octopus.actus.connector.jdbc.DbType;
import com.octopus.actus.connector.jdbc.JdbcProperties;
import com.octopus.actus.connector.jdbc.mapper.DQLMapper;
import com.octopus.actus.connector.jdbc.model.SelectStatement;
import com.octopus.actus.connector.jdbc.service.DataWarehouseDQLService;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

public class DataWarehouseDQLServiceImpl extends AbstractSqlExecutor
    implements DataWarehouseDQLService {

  public DataWarehouseDQLServiceImpl(String name, DataSource dataSource, DbType dbType) {
    super(name, dataSource, dbType);
  }

  public DataWarehouseDQLServiceImpl(JdbcProperties properties) {
    super(properties);
  }

  @Override
  public List<Map<String, Object>> queryList(SelectStatement statement) {
    return execute(() -> dqlDao.queryList(statement.toSQL(), DQLMapper.toParamMap(statement)));
  }

  @Override
  public Page<Map<String, Object>> queryPage(
      SelectStatement statement, Integer pageNum, Integer pageSize) {
    return PageHelper.startPage(pageNum, pageSize)
        .doSelectPage(() -> dqlDao.queryList(statement.toSQL(), DQLMapper.toParamMap(statement)));
  }

  @Override
  public PageInfo<Map<String, Object>> queryPageInfo(
      SelectStatement statement, Integer pageNum, Integer pageSize) {
    PageHelper.startPage(pageNum, pageSize);
    List<Map<String, Object>> result =
        execute(() -> dqlDao.queryList(statement.toSQL(), DQLMapper.toParamMap(statement)));
    return PageInfo.of(result);
  }

  @Override
  public List<Map<String, Object>> queryLimit(
      SelectStatement statement, Integer pageNum, Integer pageSize) {
    PageHelper.startPage(pageNum, pageSize);
    return execute(() -> dqlDao.queryList(statement.toSQL(), DQLMapper.toParamMap(statement)));
  }
}
