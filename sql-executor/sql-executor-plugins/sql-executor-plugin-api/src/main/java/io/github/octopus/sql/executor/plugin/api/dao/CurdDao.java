package io.github.octopus.sql.executor.plugin.api.dao;

import com.baomidou.mybatisplus.core.metadata.IPage;
import io.github.octopus.sql.executor.core.entity.Delete;
import io.github.octopus.sql.executor.core.entity.Insert;
import io.github.octopus.sql.executor.core.entity.Select;
import io.github.octopus.sql.executor.core.entity.Update;
import io.github.octopus.sql.executor.core.entity.Upsert;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.session.RowBounds;

public interface CurdDao {
  int save(Insert insert);

  int saveBatch(Insert insert);

  int upsert(Upsert upsert);

  int update(Update update, @Param("params") Map<String, Object> params);

  int delete(Delete delete, @Param("params") Map<String, Object> params);

  List<Map<String, Object>> queryList(Select select);

  IPage<Map<String, Object>> queryPage(Select select);

  List<Map<String, Object>> queryListByLimit(Select select, RowBounds rowBounds);

  int count(
      @Param("database") String database,
      @Param("table") String table,
      @Param("where") String where);
}
