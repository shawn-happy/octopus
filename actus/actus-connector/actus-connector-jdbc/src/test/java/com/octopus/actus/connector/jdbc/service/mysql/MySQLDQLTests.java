package com.octopus.actus.connector.jdbc.service.mysql;

import com.github.pagehelper.PageHelper;
import com.octopus.actus.connector.jdbc.SqlSessionProvider;
import com.octopus.actus.connector.jdbc.dao.dialect.mysql.MySQLDQLDao;
import com.octopus.actus.connector.jdbc.service.DataWarehouseDQLService;
import com.octopus.actus.connector.jdbc.service.impl.DataWarehouseDQLServiceImpl;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class MySQLDQLTests extends MySQLTestsCommon {

  private DataWarehouseDQLService dataWarehouseDQLService;

  @BeforeEach
  public void init() {
    dataWarehouseDQLService = new DataWarehouseDQLServiceImpl(properties);
  }

  @Test
  public void testQueryList() {
    String sql = "select * from information_schema.TABLES";
    SqlSession sqlSession = SqlSessionProvider.createSqlSession(properties);
    MySQLDQLDao mapper = sqlSession.getMapper(MySQLDQLDao.class);
    PageHelper.startPage(0, 10);
    List<Map<String, Object>> maps = mapper.queryList(sql, null);
    Assertions.assertNotNull(maps);
    Assertions.assertEquals(10, maps.size());
  }
}
