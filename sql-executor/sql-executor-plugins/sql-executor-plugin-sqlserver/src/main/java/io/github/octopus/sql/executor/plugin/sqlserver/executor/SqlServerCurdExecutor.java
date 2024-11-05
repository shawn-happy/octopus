package io.github.octopus.sql.executor.plugin.sqlserver.executor;

import io.github.octopus.sql.executor.core.model.Page;
import io.github.octopus.sql.executor.core.model.curd.DeleteStatement;
import io.github.octopus.sql.executor.core.model.curd.InsertStatement;
import io.github.octopus.sql.executor.core.model.curd.UpdateStatement;
import io.github.octopus.sql.executor.core.model.curd.UpsertStatement;
import io.github.octopus.sql.executor.core.model.schema.TableDefinition;
import io.github.octopus.sql.executor.plugin.api.executor.AbstractCurdExecutor;
import io.github.octopus.sql.executor.plugin.sqlserver.dialect.SqlServerCurdStatement;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class SqlServerCurdExecutor extends AbstractCurdExecutor {

  public SqlServerCurdExecutor(DataSource dataSource) {
    super(dataSource, SqlServerCurdStatement.getCurdStatement());
  }

  @Override
  public int save(InsertStatement insertStatement) {
    return 0;
  }

  @Override
  public int saveBatch(InsertStatement insertStatement) {
    return 0;
  }

  @Override
  public int upsert(TableDefinition definition, UpsertStatement upsertStatement) {
    return 0;
  }

  @Override
  public int update(UpdateStatement updateStatement) {
    return 0;
  }

  @Override
  public int delete(DeleteStatement deleteStatement) {
    return 0;
  }

  @Override
  public List<Map<String, Object>> queryListBySQL(String sql, Map<String, Object> params) {
    return List.of();
  }

  @Override
  public Page<Map<String, Object>> queryPageBySQL(
      String sql, Map<String, Object> params, Integer pageNum, Integer pageSize) {
    return null;
  }

  @Override
  public List<Map<String, Object>> queryLimitBySQL(
      String sql, Map<String, Object> params, Integer pageNum, Integer pageSize) {
    return List.of();
  }

  @Override
  public int count(String database, String table, String where) {
    return 0;
  }
}
