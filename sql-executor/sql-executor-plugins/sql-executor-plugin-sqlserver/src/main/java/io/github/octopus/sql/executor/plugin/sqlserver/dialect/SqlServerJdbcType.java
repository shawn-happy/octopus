package io.github.octopus.sql.executor.plugin.sqlserver.dialect;

import com.baomidou.mybatisplus.annotation.DbType;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcType;

public class SqlServerJdbcType implements JdbcType {

  private static final SqlServerJdbcType INSTANCE = new SqlServerJdbcType();

  private SqlServerJdbcType() {}

  public static JdbcType getJdbcType() {
    return INSTANCE;
  }

  @Override
  public String getType() {
    return "";
  }

  @Override
  public DbType getDbType() {
    return null;
  }
}
