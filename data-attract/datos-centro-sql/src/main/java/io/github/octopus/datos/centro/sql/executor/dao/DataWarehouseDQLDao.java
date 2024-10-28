package io.github.octopus.datos.centro.sql.executor.dao;

import com.baomidou.mybatisplus.core.metadata.IPage;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.session.RowBounds;

public interface DataWarehouseDQLDao {

  List<Map<String, Object>> queryList(@Param("sql") String sql, Map<String, Object> params);

  IPage<Map<String, Object>> queryPage(
      @Param("sql") String sql, Map<String, Object> params, IPage<Map<String, Object>> page);

  List<Map<String, Object>> queryListByLimit(
      @Param("sql") String sql, Map<String, Object> params, RowBounds rowBounds);

  int count(
      @Param("database") String database,
      @Param("table") String table,
      @Param("where") String where);
}
