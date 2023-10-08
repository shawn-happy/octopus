package com.octopus.actus.connector.jdbc.model;

import com.octopus.actus.connector.jdbc.model.op.InternalRelationalOp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class WhereExpressionTests {

  @Test
  public void testEq() {
    WhereExpression expression =
        WhereSimpleExpression.builder()
            .label("name")
            .relationalOp(InternalRelationalOp.EQ)
            .paramValues(
                new ParamValue[] {
                  ParamValue.builder().param("name").values(new String[] {"Shawn"}).build()
                })
            .build();
    printWhereExpr(expression);
  }

  @Test
  public void testGT() {
    WhereExpression statement =
        WhereSimpleExpression.builder()
            .label("age")
            .relationalOp(InternalRelationalOp.GT)
            .paramValues(
                new ParamValue[] {
                  ParamValue.builder().param("age").values(new String[] {"20"}).build()
                })
            .build();
    printWhereExpr(statement);
  }

  @Test
  public void testLT() {
    WhereExpression statement =
        WhereSimpleExpression.builder()
            .label("age")
            .relationalOp(InternalRelationalOp.LT)
            .paramValues(
                new ParamValue[] {
                  ParamValue.builder().param("age").values(new String[] {"20"}).build()
                })
            .build();
    printWhereExpr(statement);
  }

  @Test
  public void testGTE() {
    WhereExpression statement =
        WhereSimpleExpression.builder()
            .label("age")
            .relationalOp(InternalRelationalOp.GE)
            .paramValues(
                new ParamValue[] {
                  ParamValue.builder().param("age").values(new String[] {"20"}).build()
                })
            .build();
    printWhereExpr(statement);
  }

  @Test
  public void testLTE() {
    WhereExpression statement =
        WhereSimpleExpression.builder()
            .label("age")
            .relationalOp(InternalRelationalOp.LE)
            .paramValues(
                new ParamValue[] {
                  ParamValue.builder().param("age").values(new String[] {"20"}).build()
                })
            .build();
    printWhereExpr(statement);
  }

  @Test
  public void testLTGT() {
    WhereExpression statement =
        WhereSimpleExpression.builder()
            .label("age")
            .relationalOp(InternalRelationalOp.LTGT)
            .paramValues(
                new ParamValue[] {
                  ParamValue.builder().param("age").values(new String[] {"20"}).build()
                })
            .build();
    printWhereExpr(statement);
  }

  @Test
  public void testNE() {
    WhereExpression statement =
        WhereSimpleExpression.builder()
            .label("age")
            .relationalOp(InternalRelationalOp.NE)
            .paramValues(
                new ParamValue[] {
                  ParamValue.builder().param("age").values(new String[] {"20"}).build()
                })
            .build();
    printWhereExpr(statement);
  }

  @Test
  public void testIsNull() {
    WhereExpression statement =
        WhereSimpleExpression.builder()
            .label("name")
            .relationalOp(InternalRelationalOp.IS_NULL)
            .build();
    printWhereExpr(statement);
  }

  @Test
  public void testIsNotNull() {
    WhereExpression statement =
        WhereSimpleExpression.builder()
            .label("name")
            .relationalOp(InternalRelationalOp.IS_NOT_NULL)
            .build();
    printWhereExpr(statement);
  }

  @Test
  public void testLike() {
    WhereExpression statement =
        WhereSimpleExpression.builder()
            .label("name")
            .relationalOp(InternalRelationalOp.LIKE)
            .paramValues(
                new ParamValue[] {
                  ParamValue.builder().param("name").values(new String[] {"%Shawn%"}).build()
                })
            .build();
    printWhereExpr(statement);
  }

  @Test
  public void testNotLike() {
    WhereExpression statement =
        WhereSimpleExpression.builder()
            .label("name")
            .relationalOp(InternalRelationalOp.NOT_LIKE)
            .paramValues(
                new ParamValue[] {
                  ParamValue.builder().param("name").values(new String[] {"%Shawn%"}).build()
                })
            .build();
    printWhereExpr(statement);
  }

  @Test
  public void testIn() {
    WhereExpression statement =
        WhereSimpleExpression.builder()
            .label("name")
            .relationalOp(InternalRelationalOp.IN)
            .paramValues(
                new ParamValue[] {
                  ParamValue.builder()
                      .param("names")
                      .values(new String[] {"Shawn", "Jack", "Bob", "Jackson", "Bill", "Nio"})
                      .build()
                })
            .build();
    printWhereExpr(statement);
  }

  @Test
  public void testNotIn() {
    WhereExpression statement =
        WhereSimpleExpression.builder()
            .label("name")
            .relationalOp(InternalRelationalOp.NOT_IN)
            .paramValues(
                new ParamValue[] {
                  ParamValue.builder()
                      .param("names")
                      .values(new String[] {"Shawn", "Jack", "Bob", "Jackson", "Bill", "Nio"})
                      .build()
                })
            .build();
    printWhereExpr(statement);
  }

  @Test
  public void testBetweenAnd() {
    WhereExpression statement =
        WhereSimpleExpression.builder()
            .label("create_time")
            .relationalOp(InternalRelationalOp.BETWEEN_AND)
            .paramValues(
                new ParamValue[] {
                  ParamValue.builder()
                      .param("startTime")
                      .values(new String[] {"2022-10-08 00:00:00"})
                      .build(),
                  ParamValue.builder()
                      .param("endTime")
                      .values(
                          new String[] {
                            LocalDateTime.now()
                                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                          })
                      .build()
                })
            .build();
    printWhereExpr(statement);
  }

  @Test
  public void testLogicalExpressionAnd() {
    WhereExpression left =
        WhereSimpleExpression.builder()
            .label("name")
            .relationalOp(InternalRelationalOp.NOT_IN)
            .paramValues(
                new ParamValue[] {
                  ParamValue.builder()
                      .param("names")
                      .values(new String[] {"Shawn", "Jack", "Bob", "Jackson", "Bill", "Nio"})
                      .build()
                })
            .build();
    WhereExpression right =
        WhereSimpleExpression.builder()
            .label("age")
            .relationalOp(InternalRelationalOp.GT)
            .paramValues(
                new ParamValue[] {
                  ParamValue.builder().param("age").values(new String[] {"20"}).build()
                })
            .build();
    WhereExpression logicalExpression = new WhereLogicalExpression().and(left, right);
    printWhereExpr(logicalExpression);
  }

  @Test
  public void testLogicalExpressionOr() {
    WhereExpression left =
        WhereSimpleExpression.builder()
            .label("name")
            .relationalOp(InternalRelationalOp.NOT_IN)
            .paramValues(
                new ParamValue[] {
                  ParamValue.builder()
                      .param("names")
                      .values(new String[] {"Shawn", "Jack", "Bob", "Jackson", "Bill", "Nio"})
                      .build()
                })
            .build();
    WhereExpression right =
        WhereSimpleExpression.builder()
            .label("age")
            .relationalOp(InternalRelationalOp.GT)
            .paramValues(
                new ParamValue[] {
                  ParamValue.builder().param("age").values(new String[] {"20"}).build()
                })
            .build();
    WhereExpression logicalExpression = new WhereLogicalExpression().or(left, right);
    printWhereExpr(logicalExpression);
  }

  @Test
  public void testLogicalComplexExpression() {
    WhereExpression left =
        WhereSimpleExpression.builder()
            .label("name")
            .relationalOp(InternalRelationalOp.EQ)
            .paramValues(
                new ParamValue[] {
                  ParamValue.builder().param("name").values(new String[] {"Shawn"}).build()
                })
            .build();
    WhereExpression right =
        WhereSimpleExpression.builder()
            .label("age")
            .relationalOp(InternalRelationalOp.GT)
            .paramValues(
                new ParamValue[] {
                  ParamValue.builder().param("age").values(new String[] {"20"}).build()
                })
            .build();
    // name = 'Shawn' or age > '20'
    WhereExpression logicalExpression1 = new WhereLogicalExpression().or(left, right);
    // name = 'Shawn' and age > '20'
    WhereExpression logicExpression2 = new WhereLogicalExpression().and(left, right);
    WhereSimpleExpression expression =
        WhereSimpleExpression.builder()
            .label("create_time")
            .relationalOp(InternalRelationalOp.BETWEEN_AND)
            .paramValues(
                new ParamValue[] {
                  ParamValue.builder()
                      .param("startTime")
                      .values(new String[] {"2022-10-08 00:00:00"})
                      .build(),
                  ParamValue.builder()
                      .param("endTime")
                      .values(
                          new String[] {
                            LocalDateTime.now()
                                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                          })
                      .build()
                })
            .build();
    //   ( ( (name  = 'Shawn' ) OR (age  > '20' ) ) AND (create_time  BETWEEN '2022-10-08 00:00:00'
    // AND '2023-10-08 14:53:46' ) ) OR ( (name  = 'Shawn' ) AND (age  > '20' ) )
    printWhereExpr(
        new WhereLogicalExpression()
            .or(
                new WhereLogicalExpression()
                    .and(
                        logicalExpression1,
                        expression), // ( name = 'Shawn' or age > '20'  and create_time  BETWEEN
                // '2022-10-08 00:00:00' AND '2023-10-08 14:53:46')
                logicExpression2));
  }

  private void printWhereExpr(WhereExpression expression) {
    System.out.println(expression.toSQL());
    System.out.println(expression.toSQLValue());
  }
}
