package io.github.octopus.datos.centro.sql.executor.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import io.github.octopus.datos.centro.sql.model.SelectStatement;
import java.util.List;
import java.util.Map;

public interface DataWarehouseDQLService {
  List<Map<String, Object>> queryList(SelectStatement statement);

  IPage<Map<String, Object>> queryPage(
      SelectStatement statement, Integer pageNum, Integer pageSize);

  List<Map<String, Object>> queryLimit(
      SelectStatement statement, Integer pageNum, Integer pageSize);
}
