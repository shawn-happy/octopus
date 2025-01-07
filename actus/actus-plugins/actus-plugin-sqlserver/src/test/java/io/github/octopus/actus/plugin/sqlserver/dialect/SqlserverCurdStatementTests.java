package io.github.octopus.actus.plugin.sqlserver.dialect;

import io.github.octopus.actus.plugin.api.dialect.CurdStatement;
import org.junit.jupiter.api.Test;

public class SqlserverCurdStatementTests {
  @Test
  public void testPageSql() {
    String sql = "select * from sys.tables";
    CurdStatement curdStatement = SqlServerCurdStatement.getCurdStatement();
    String s = curdStatement.buildPageSql(sql, 10, 5);
    System.out.println(s);
  }
}
