package io.github.octopus.sql.executor.plugin.api.executor;

import io.github.octopus.sql.executor.core.entity.Select;
import io.github.octopus.sql.executor.core.exception.SqlExecuteException;
import io.github.octopus.sql.executor.core.model.curd.DeleteStatement;
import io.github.octopus.sql.executor.core.model.curd.InsertStatement;
import io.github.octopus.sql.executor.core.model.curd.UpdateStatement;
import io.github.octopus.sql.executor.core.model.curd.UpsertStatement;
import io.github.octopus.sql.executor.core.model.schema.TableDefinition;
import io.github.octopus.sql.executor.plugin.api.mapper.CurdMapper;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractCurdExecutor implements CurdExecutor {

  private final String name;

  protected AbstractCurdExecutor(String name, DataSource dataSource) {
    this.name = name;

  }

  @Override
  public int save(InsertStatement insertStatement) {
  }

  @Override
  public int saveBatch(InsertStatement insertStatement) {
    return ;
  }

  @Override
  public int upsert(TableDefinition definition, UpsertStatement upsertStatement) {
    return ;
  }

  @Override
  public int update(UpdateStatement updateStatement) {
    return executeCurd(
        curd ->
            curd.update(
                CurdMapper.toUpdate(updateStatement),
                CurdMapper.toParamMap(updateStatement.getExpression())));
  }

  @Override
  public int delete(DeleteStatement deleteStatement) {
    return executeCurd(
        curd ->
            curd.delete(
                CurdMapper.toDelete(deleteStatement),
                CurdMapper.toParamMap(deleteStatement.getExpression())));
  }

  @Override
  public List<Map<String, Object>> queryListBySQL(String sql, Map<String, Object> params) {
    return executeCurd(curd -> curd.queryList(Select.builder().sql(sql).params(params).build()));
  }

  @Override
  public IPage<Map<String, Object>> queryPageBySQL(
      String sql, Map<String, Object> params, Integer pageNum, Integer pageSize) {
    return executeCurd(curd -> curd.queryPage(Select.builder().sql(sql).params(params).build()));
  }

  @Override
  public List<Map<String, Object>> queryLimitBySQL(
      String sql, Map<String, Object> params, Integer pageNum, Integer pageSize) {
    return executeCurd(curd -> curd.queryList(Select.builder().sql(sql).params(params).build()));
  }

  @Override
  public int count(String database, String table, String where) {
    return executeCurd(curd -> curd.count(database, table, where));
  }

  private <R> R executeCurd(Function<CurdDao, R> function) {
    try {
      return function.apply(curdDao);
    } catch (Exception e) {
      throw new SqlExecuteException(e);
    } finally {
      SqlSessionProvider.releaseSqlSessionManager(name);
    }
  }
}
