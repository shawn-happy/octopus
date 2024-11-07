package io.github.octopus.sql.executor.plugin.api.executor;

import io.github.octopus.sql.executor.core.PageSqlUtils;
import io.github.octopus.sql.executor.core.model.DatabaseIdentifier;
import io.github.octopus.sql.executor.core.model.Page;
import io.github.octopus.sql.executor.core.model.curd.DeleteStatement;
import io.github.octopus.sql.executor.core.model.curd.InsertStatement;
import io.github.octopus.sql.executor.core.model.curd.UpdateStatement;
import io.github.octopus.sql.executor.core.model.curd.UpsertStatement;
import io.github.octopus.sql.executor.core.model.schema.TableDefinition;
import io.github.octopus.sql.executor.plugin.api.dialect.CurdStatement;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractCurdExecutor extends AbstractExecutor implements CurdExecutor {

  private final CurdStatement curdStatement;

  protected AbstractCurdExecutor(DataSource dataSource, CurdStatement curdStatement) {
    super(dataSource);
    this.curdStatement = curdStatement;
  }

  @Override
  public int save(InsertStatement insertStatement) {
    String insertSql = curdStatement.getInsertSql(insertStatement);
    log.info("insert statement: {}", insertSql);
    Map<String, Object> paramValues = new LinkedHashMap<>();
    List<String> columns = insertStatement.getColumns();
    Object[] values = insertStatement.getValues().get(0);
    for (int i = 0; i < columns.size(); i++) {
      String col = columns.get(i);
      Object value = values[i];
      paramValues.put(col, value);
    }
    return getProcessor().executeUpdate(insertSql, paramValues);
  }

  @Override
  public int update(UpdateStatement updateStatement) {
    String updateSql = curdStatement.getUpdateSql(updateStatement);
    log.info("update statement: {}", updateSql);
    return getProcessor().executeUpdate(updateSql, null);
  }

  @Override
  public int delete(DeleteStatement deleteStatement) {
    String deleteSql = curdStatement.getDeleteSql(deleteStatement);
    log.info("delete statement: {}", deleteSql);
    return getProcessor().executeUpdate(deleteSql, null);
  }

  @Override
  public int saveBatch(InsertStatement insertStatement) {
    JdbcDialect jdbcDialect = curdStatement.getJdbcDialect();
    String dialectName = jdbcDialect.getDialectName();
    if (DatabaseIdentifier.MYSQL.equals(dialectName)) {

    } else {

    }
    return 0;
  }

  @Override
  public int upsert(TableDefinition definition, UpsertStatement upsertStatement) {
    return 0;
  }

  @Override
  public int count(String database, String table, String where) {
    return 0;
  }

  @Override
  public Page<Map<String, Object>> queryPageBySQL(
      String sql, Map<String, Object> params, long pageNum, long pageSize) {
    String pageSql = curdStatement.buildPageSql(sql, pageNum * pageSize, pageSize);
    String countSql = PageSqlUtils.autoCountSql(sql);
    Long count = getProcessor().executeQueryObject(countSql, null, Long.class);
    List<Map<String, Object>> maps = getProcessor().executeQueryList(pageSql, params);
    return Page.<Map<String, Object>>builder()
        .pageNum(pageNum)
        .pageSize(pageSize)
        .total(count)
        .pageCount(count / pageSize)
        .records(maps)
        .build();
  }

  @Override
  public List<Map<String, Object>> queryListBySQL(String sql, Map<String, Object> params) {
    return List.of();
  }

  @Override
  public List<Map<String, Object>> queryLimitBySQL(
      String sql, Map<String, Object> params, long pageNum, long pageSize) {
    return List.of();
  }
}
