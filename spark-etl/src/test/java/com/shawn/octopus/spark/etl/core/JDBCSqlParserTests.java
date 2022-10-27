package com.shawn.octopus.spark.etl.core;

import com.shawn.octopus.spark.etl.core.util.JDBCSqlParser;
import org.junit.jupiter.api.Test;

public class JDBCSqlParserTests {

  @Test
  public void testSqlParser() {
    JDBCSqlParser parser = new JDBCSqlParser("${", "}");
    String sql = "select * from user where id = ${user_id} and name = ${user_name}";
    String parse = parser.parse(sql);
    System.out.println(parse);
  }
}
