package io.github.octopus.actus.plugin.api.executor;

import io.github.octopus.actus.core.PageSqlUtils;
import io.github.octopus.actus.core.model.Page;
import io.github.octopus.actus.core.model.curd.DeleteStatement;
import io.github.octopus.actus.core.model.curd.InsertStatement;
import io.github.octopus.actus.core.model.curd.RowExistsStatement;
import io.github.octopus.actus.core.model.curd.UpdateStatement;
import io.github.octopus.actus.core.model.curd.UpsertStatement;
import io.github.octopus.actus.core.model.schema.TableDefinition;
import io.github.octopus.actus.core.model.schema.TablePath;
import io.github.octopus.actus.plugin.api.dialect.CurdStatement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  public int insert(InsertStatement insertStatement) {
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
    return getProcessor().update(insertSql, paramValues);
  }

  @Override
  public int update(UpdateStatement updateStatement) {
    String updateSql = curdStatement.getUpdateSql(updateStatement);
    return getProcessor().update(updateSql, null);
  }

  @Override
  public int delete(DeleteStatement deleteStatement) {
    String deleteSql = curdStatement.getDeleteSql(deleteStatement);
    return getProcessor().update(deleteSql, null);
  }

  @Override
  public int insertBatch(InsertStatement insertStatement) {
    Optional<String> insertBatchSql = curdStatement.getInsertBatchSql(insertStatement);
    if (insertBatchSql.isPresent()) {
      String sql = insertBatchSql.get();
      return getProcessor().update(sql, null);
    } else {
      String insertSql = curdStatement.getInsertSql(insertStatement);
      return getProcessor().updateBatch(insertSql, insertStatement.getValues());
    }
  }

  @Override
  public int upsert(TableDefinition definition, UpsertStatement upsertStatement) {
    Optional<String> upsertSql = curdStatement.getUpsertSql(upsertStatement);
    if (upsertSql.isPresent()) {
      String sql = upsertSql.get();
      return getProcessor().update(sql, null);
    }
    RowExistsStatement rowExistsStatement =
        RowExistsStatement.builder()
            .tablePath(upsertStatement.getTablePath())
            .expression(null)
            .build();
    String rowExistsSql = curdStatement.getRowExistsSql(rowExistsStatement);
    Object rowRecord = getProcessor().queryOne(rowExistsSql, Object.class);
    if (rowRecord != null) {
      UpdateStatement build = UpdateStatement.builder().build();
      String updateSql = curdStatement.getUpdateSql(build);
      return getProcessor().update(updateSql, null);
    } else {
      InsertStatement insertStatement = InsertStatement.builder().build();
      String insertSql = curdStatement.getInsertSql(insertStatement);
      return getProcessor().update(insertSql, null);
    }
  }

  @Override
  public void truncate(String database, String schema, String table) {
    String truncateTableSql =
        curdStatement.getTruncateTableSql(TablePath.of(database, schema, table));
    getProcessor().update(truncateTableSql, null);
  }

  @Override
  public Map<String, Object> queryOneBySql(String sql, Map<String, Object> params) {
    return getProcessor().queryOne(sql, params);
  }

  @Override
  public Page<Map<String, Object>> queryPageBySql(
      String sql, Map<String, Object> params, long pageNum, long pageSize) {
    String countSql = PageSqlUtils.autoCountSql(sql);
    Long count = getProcessor().queryOne(countSql, params, Long.class);
    long pageCount = count / pageSize;
    if (count > pageSize * pageCount) {
      pageCount++;
    }
    Page<Map<String, Object>> page =
        Page.<Map<String, Object>>builder()
            .pageNum(pageNum)
            .pageSize(pageSize)
            .total(count)
            .pageCount(pageCount)
            .build();

    if (pageNum > pageCount) {
      return page;
    }
    String pageSql = curdStatement.buildPageSql(sql, pageNum * pageSize, pageSize);
    List<Map<String, Object>> maps = getProcessor().queryList(pageSql, params);
    return Page.<Map<String, Object>>builder()
        .pageNum(pageNum)
        .pageSize(pageSize)
        .total(count)
        .pageCount(pageCount)
        .records(maps)
        .build();
  }

  @Override
  public List<Map<String, Object>> queryListBySql(String sql, Map<String, Object> params) {
    return getProcessor().queryList(sql, params);
  }
}
