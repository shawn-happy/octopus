package io.github.octopus.sql.executor.plugin.oracle.dialect;

import com.baomidou.mybatisplus.annotation.DbType;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcType;

public class OracleJdbcType implements JdbcType {

  private static final OracleJdbcType INSTANCE = new OracleJdbcType();

  private OracleJdbcType() {}

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
