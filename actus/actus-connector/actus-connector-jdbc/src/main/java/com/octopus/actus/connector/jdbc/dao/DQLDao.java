package com.octopus.actus.connector.jdbc.dao;

import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.session.RowBounds;

public interface DQLDao {

  List<Map<String, Object>> queryList(@Param("sql") String sql, Map<String, Object> params);

  List<Map<String, Object>> queryListByLimit(
      @Param("sql") String sql, Map<String, Object> params, RowBounds rowBounds);

  int count(
      @Param("database") String database,
      @Param("table") String table,
      @Param("where") String where);
}
