package io.github.octopus.sql.executor.plugin.api.executor;

import com.baomidou.mybatisplus.core.metadata.IPage;
import io.github.octopus.sql.executor.core.entity.Select;
import io.github.octopus.sql.executor.core.model.curd.DeleteStatement;
import io.github.octopus.sql.executor.core.model.curd.InsertStatement;
import io.github.octopus.sql.executor.core.model.curd.UpdateStatement;
import io.github.octopus.sql.executor.core.model.curd.UpsertStatement;
import io.github.octopus.sql.executor.core.model.schema.ColumnInfo;
import io.github.octopus.sql.executor.plugin.api.mapper.CurdMapper;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class CurdExecutor extends AbstractSqlExecutor {

  protected CurdExecutor(String name, DataSource dataSource) {
    super(name, dataSource);
  }

  public int save(InsertStatement insertStatement) {
    return executeCurd(curd -> curd.save(CurdMapper.toInsert(insertStatement)));
  }

  public int saveBatch(InsertStatement insertStatement) {
    return executeCurd(curd -> curd.saveBatch(CurdMapper.toInsert(insertStatement)));
  }

  public int upsert(List<ColumnInfo> columnInfos, UpsertStatement upsertStatement) {
    return executeCurd(curd -> curd.upsert(CurdMapper.toUpsert(columnInfos, upsertStatement)));
  }

  public int update(UpdateStatement updateStatement) {
    return executeCurd(
        curd ->
            curd.update(
                CurdMapper.toUpdate(updateStatement),
                CurdMapper.toParamMap(updateStatement.getExpression())));
  }

  public int delete(DeleteStatement deleteStatement) {
    return executeCurd(
        curd ->
            curd.delete(
                CurdMapper.toDelete(deleteStatement),
                CurdMapper.toParamMap(deleteStatement.getExpression())));
  }

  public List<Map<String, Object>> queryListBySQL(String sql, Map<String, Object> params) {
    return executeCurd(curd -> curd.queryList(Select.builder().sql(sql).params(params).build()));
  }

  public IPage<Map<String, Object>> queryPageBySQL(
      String sql, Map<String, Object> params, Integer pageNum, Integer pageSize) {
    return executeCurd(curd -> curd.queryPage(Select.builder().sql(sql).params(params).build()));
  }

  public List<Map<String, Object>> queryLimitBySQL(
      String sql, Map<String, Object> params, Integer pageNum, Integer pageSize) {
    return executeCurd(curd -> curd.queryList(Select.builder().sql(sql).params(params).build()));
  }

  public int count(String database, String table, String where) {
    return executeCurd(curd -> curd.count(database, table, where));
  }
}
