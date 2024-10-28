package io.github.octopus.sql.executor.plugin.sqlserver.dao;

import com.baomidou.mybatisplus.core.metadata.IPage;
import io.github.octopus.sql.executor.core.entity.Delete;
import io.github.octopus.sql.executor.core.entity.Insert;
import io.github.octopus.sql.executor.core.entity.Select;
import io.github.octopus.sql.executor.core.entity.Update;
import io.github.octopus.sql.executor.plugin.api.dao.CurdDao;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.session.RowBounds;

public interface SqlServerCurdDao extends CurdDao {
  @Override
  int save(Insert insert);

  @Override
  int saveBatch(Insert insert);

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
