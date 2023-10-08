package com.octopus.actus.connector.jdbc.service;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageInfo;
import com.octopus.actus.connector.jdbc.model.SelectStatement;
import java.util.List;
import java.util.Map;

public interface DataWarehouseDQLService {
  List<Map<String, Object>> queryList(SelectStatement statement);

  Page<Map<String, Object>> queryPage(SelectStatement statement, Integer pageNum, Integer pageSize);

  PageInfo<Map<String, Object>> queryPageInfo(
      SelectStatement statement, Integer pageNum, Integer pageSize);

  List<Map<String, Object>> queryLimit(
      SelectStatement statement, Integer pageNum, Integer pageSize);
}
