package io.github.octopus.datos.centro.sql.executor.dao.dialect.mysql;

import io.github.octopus.datos.centro.sql.executor.dao.DataWarehouseDQLDao;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.session.RowBounds;

public interface MySQLDQLDao extends DataWarehouseDQLDao {

  @Override
  List<Map<String, Object>> queryList(@Param("sql") String sql, Map<String, Object> params);

  @Override
  List<Map<String, Object>> queryListByLimit(
      @Param("sql") String sql, Map<String, Object> params, RowBounds rowBounds);

  @Override
  int count(
      @Param("database") String database,
      @Param("table") String table,
      @Param("where") String where);
}
