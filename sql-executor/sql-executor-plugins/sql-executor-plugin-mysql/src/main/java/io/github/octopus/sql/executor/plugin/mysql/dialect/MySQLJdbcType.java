package io.github.octopus.sql.executor.plugin.mysql.dialect;

import com.baomidou.mybatisplus.annotation.DbType;
import io.github.octopus.sql.executor.core.model.DatabaseIdentifier;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcType;

public class MySQLJdbcType implements JdbcType {

  private static final MySQLJdbcType INSTANCE = new MySQLJdbcType();

  private MySQLJdbcType() {}

  public static JdbcType getJdbcType() {
    return INSTANCE;
  }

  @Override
  public String getType() {
    return DatabaseIdentifier.MYSQL;
  }

  @Override
  public DbType getDbType() {
    return DbType.MYSQL;
  }
}
