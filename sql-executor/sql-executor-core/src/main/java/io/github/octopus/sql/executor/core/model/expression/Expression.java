package io.github.octopus.sql.executor.core.model.expression;

public interface Expression {
  String toSQL();

  String toSQLValue();
}
