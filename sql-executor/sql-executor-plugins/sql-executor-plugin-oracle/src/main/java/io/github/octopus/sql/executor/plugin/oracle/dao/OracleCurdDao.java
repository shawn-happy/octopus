package io.github.octopus.sql.executor.plugin.oracle.dao;

import com.baomidou.mybatisplus.core.metadata.IPage;
import io.github.octopus.sql.executor.core.entity.Delete;
import io.github.octopus.sql.executor.core.entity.Insert;
import io.github.octopus.sql.executor.core.entity.Select;
import io.github.octopus.sql.executor.core.entity.Update;
import io.github.octopus.sql.executor.core.entity.Upsert;
import io.github.octopus.sql.executor.plugin.api.dao.CurdDao;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.session.RowBounds;

public interface OracleCurdDao extends CurdDao {
  @Override
  int save(Insert insert);

  @Override
  default int saveBatch(Insert insert) {
    List<Map<String, Object>> batchParams = insert.getBatchParams();
    int insertCount = 0;
    for (Map<String, Object> batchParam : batchParams) {
      Insert newInsert =
          Insert.builder()
              .database(insert.getDatabase())
              .table(insert.getTable())
              .columns(insert.getColumns())
              .params(batchParam)
              .build();
      insertCount += save(newInsert);
    }
    return insertCount;
  }

  @Override
  int upsert(Upsert upsert);

  @Override
  int update(@Param("update") Update update, @Param("params") Map<String, Object> params);

  @Override
  int delete(@Param("delete") Delete delete, @Param("params") Map<String, Object> params);

  @Override
  List<Map<String, Object>> queryList(Select select);

  @Override
  IPage<Map<String, Object>> queryPage(Select select);

  @Override
  List<Map<String, Object>> queryListByLimit(Select select, RowBounds rowBounds);

  @Override
  int count(
      @Param("database") String database,
      @Param("table") String table,
      @Param("where") String where);
}
