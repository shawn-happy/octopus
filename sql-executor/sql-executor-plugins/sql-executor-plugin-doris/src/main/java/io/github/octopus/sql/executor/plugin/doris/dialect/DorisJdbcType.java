package io.github.octopus.sql.executor.plugin.doris.dialect;

import com.baomidou.mybatisplus.annotation.DbType;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcType;

public class DorisJdbcType implements JdbcType {

  private static final DorisJdbcType INSTANCE = new DorisJdbcType();

  private DorisJdbcType() {}

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
