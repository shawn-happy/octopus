package io.github.octopus.sql.executor.plugin.api.executor;

import io.github.octopus.sql.executor.core.model.Page;
import io.github.octopus.sql.executor.core.model.curd.DeleteStatement;
import io.github.octopus.sql.executor.core.model.curd.InsertStatement;
import io.github.octopus.sql.executor.core.model.curd.UpdateStatement;
import io.github.octopus.sql.executor.core.model.curd.UpsertStatement;
import io.github.octopus.sql.executor.core.model.schema.TableDefinition;
import java.util.List;
import java.util.Map;

public interface CurdExecutor {

  int save(InsertStatement insertStatement);

  int saveBatch(InsertStatement insertStatement);

  int upsert(TableDefinition definition, UpsertStatement upsertStatement);

  /**
   * 如果是Doris 当前 UPDATE 语句仅支持在 UNIQUE KEY 模型上的行更新。
   *
   * @param updateStatement
   * @return
   */
  int update(UpdateStatement updateStatement);

  int delete(DeleteStatement deleteStatement);

  /**
   * @param sql 暂不支持mybatis的动态sql
   * @param params
   * @return
   */
  List<Map<String, Object>> queryListBySQL(String sql, Map<String, Object> params);

  /**
   * @param sql 暂不支持mybatis的动态sql
   * @param params
   * @return
   */
  Page<Map<String, Object>> queryPageBySQL(
      String sql, Map<String, Object> params, long pageNum, long pageSize);

  /**
   * @param sql 暂不支持mybatis的动态sql
   * @param params
   * @return
   */
  List<Map<String, Object>> queryLimitBySQL(
      String sql, Map<String, Object> params, long pageNum, long pageSize);

  int count(String database, String table, String where);
}
