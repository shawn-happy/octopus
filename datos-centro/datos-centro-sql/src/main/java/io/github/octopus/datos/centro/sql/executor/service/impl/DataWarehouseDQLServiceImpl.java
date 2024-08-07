package io.github.octopus.datos.centro.sql.executor.service.impl;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import io.github.octopus.datos.centro.sql.executor.JDBCDataSourceProperties;
import io.github.octopus.datos.centro.sql.executor.mapper.DQLMapper;
import io.github.octopus.datos.centro.sql.executor.service.DataWarehouseDQLService;
import io.github.octopus.datos.centro.sql.model.SelectStatement;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.session.RowBounds;

public class DataWarehouseDQLServiceImpl extends AbstractSqlExecutor
    implements DataWarehouseDQLService {

  public DataWarehouseDQLServiceImpl(JDBCDataSourceProperties properties) {
    super(properties);
  }

  @Override
  public List<Map<String, Object>> queryList(SelectStatement statement) {
    return execute(
        () -> dataWarehouseDQLDao.queryList(statement.toSQL(), DQLMapper.toParamMap(statement)));
  }

  @Override
  public IPage<Map<String, Object>> queryPage(
      SelectStatement statement, Integer pageNum, Integer pageSize) {
    Page<Map<String, Object>> page = new Page<>(pageNum, pageSize);
    return dataWarehouseDQLDao.queryPage(statement.toSQL(), DQLMapper.toParamMap(statement), page);
  }

  @Override
  public List<Map<String, Object>> queryLimit(
      SelectStatement statement, Integer pageNum, Integer pageSize) {
    return dataWarehouseDQLDao.queryListByLimit(
        statement.toSQL(), DQLMapper.toParamMap(statement), new RowBounds(pageNum, pageSize));
  }
}
