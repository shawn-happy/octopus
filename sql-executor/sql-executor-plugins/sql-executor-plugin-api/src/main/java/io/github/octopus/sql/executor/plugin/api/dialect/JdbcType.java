package io.github.octopus.sql.executor.plugin.api.dialect;

import com.baomidou.mybatisplus.annotation.DbType;

public interface JdbcType {

  String getType();

  DbType getDbType();
}
