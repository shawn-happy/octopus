package io.github.octopus.actus.core;

import org.junit.jupiter.api.Test;

public class PageSqlUtilsTests {
  @Test
  public void testGenerateCountSql() {
    String sql =
        "select a.* from sys.tables a left join (select * from sys.tables) b on a.id = b.id";
    String originalCountSql = PageSqlUtils.autoCountSql(sql);
    System.out.println(originalCountSql);
  }
}
