package com.octopus.actus.connector.jdbc.model;

import com.octopus.actus.connector.jdbc.model.SelectStatement.ColumnAlias;
import com.octopus.actus.connector.jdbc.model.SelectStatement.TableAlias;
import com.octopus.actus.connector.jdbc.model.op.InternalRelationalOp;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class SelectStatementTests {

  @Test
  public void testSimpleSelect() {
    SelectStatement statement =
        SelectStatement.builder()
            .columnAliases(
                Arrays.asList(
                    ColumnAlias.builder().column("id").build(),
                    ColumnAlias.builder().column("name").build(),
                    ColumnAlias.builder().column("password").build()))
            .tableAlias(TableAlias.builder().table("user").build())
            .build();
    print(statement);
  }

  @Test
  public void testSelectWithColumnAlias() {
    SelectStatement statement =
        SelectStatement.builder()
            .columnAliases(
                Arrays.asList(
                    ColumnAlias.builder().column("id").alias("pk").build(),
                    ColumnAlias.builder().column("name").build(),
                    ColumnAlias.builder().column("password").alias("pwd").build()))
            .tableAlias(TableAlias.builder().table("user").build())
            .build();
    print(statement);
  }

  @Test
  public void testSelectWithNoColumn() {
    SelectStatement statement =
        SelectStatement.builder().tableAlias(TableAlias.builder().table("user").build()).build();
    print(statement);
  }

  @Test
  public void testSelectWithTableAlias() {
    SelectStatement statement =
        SelectStatement.builder()
            .columnAliases(
                Arrays.asList(
                    ColumnAlias.builder().column("id").alias("pk").build(),
                    ColumnAlias.builder().column("name").build(),
                    ColumnAlias.builder().column("password").alias("pwd").build()))
            .tableAlias(TableAlias.builder().table("user").alias("t_user").build())
            .build();
    print(statement);
  }

  @Test
  public void testSelectWithDbTable() {
    SelectStatement statement =
        SelectStatement.builder()
            .columnAliases(
                Arrays.asList(
                    ColumnAlias.builder().column("id").alias("pk").build(),
                    ColumnAlias.builder().column("name").build(),
                    ColumnAlias.builder().column("password").alias("pwd").build()))
            .tableAlias(TableAlias.builder().database("db").table("user").build())
            .build();
    print(statement);
  }

  @Test
  public void testSelectWithDbTableAlias() {
    SelectStatement statement =
        SelectStatement.builder()
            .columnAliases(
                Arrays.asList(
                    ColumnAlias.builder().column("id").alias("pk").build(),
                    ColumnAlias.builder().column("name").build(),
                    ColumnAlias.builder().column("password").alias("pwd").build()))
            .tableAlias(TableAlias.builder().database("db").table("user").alias("db_user").build())
            .build();
    print(statement);
  }

  @Test
  public void testSelectWithWhere() {
    SelectStatement statement =
        SelectStatement.builder()
            .columnAliases(
                Arrays.asList(
                    ColumnAlias.builder().column("id").alias("pk").build(),
                    ColumnAlias.builder().column("name").build(),
                    ColumnAlias.builder().column("age").build(),
                    ColumnAlias.builder().column("password").alias("pwd").build(),
                    ColumnAlias.builder().column("createTime").alias("create_time").build()))
            .tableAlias(TableAlias.builder().database("db").table("user").alias("db_user").build())
            .where(
                WhereSimpleExpression.builder()
                    .label("age")
                    .relationalOp(InternalRelationalOp.GT)
                    .paramValues(
                        new ParamValue[] {
                          ParamValue.builder().param("age").values(new String[] {"20"}).build()
                        })
                    .build())
            .build();
    print(statement);
  }

  @Test
  public void testSelectWithWhereLogical() {
    SelectStatement statement =
        SelectStatement.builder()
            .columnAliases(
                Arrays.asList(
                    ColumnAlias.builder().column("id").alias("pk").build(),
                    ColumnAlias.builder().column("name").build(),
                    ColumnAlias.builder().column("age").build(),
                    ColumnAlias.builder().column("password").alias("pwd").build(),
                    ColumnAlias.builder().column("createTime").alias("create_time").build()))
            .tableAlias(TableAlias.builder().database("db").table("user").alias("db_user").build())
            .where(
                new WhereLogicalExpression()
                    .and(
                        WhereSimpleExpression.builder()
                            .label("age")
                            .relationalOp(InternalRelationalOp.GT)
                            .paramValues(
                                new ParamValue[] {
                                  ParamValue.builder()
                                      .param("age")
                                      .values(new String[] {"20"})
                                      .build()
                                })
                            .build(),
                        WhereSimpleExpression.builder()
                            .label("create_time")
                            .relationalOp(InternalRelationalOp.BETWEEN_AND)
                            .paramValues(
                                new ParamValue[] {
                                  ParamValue.builder()
                                      .param("startTime")
                                      .values(new String[] {"1994-02-01"})
                                      .build(),
                                  ParamValue.builder()
                                      .param("end")
                                      .values(new String[] {"2023-02-01"})
                                      .build()
                                })
                            .build()))
            .build();
    print(statement);
  }

  private void print(SelectStatement statement) {
    System.out.println(statement.toSQL());
    System.out.println(statement.toSQLValue());
  }
}
