package io.github.octopus.actus.core.model.expression;

public interface Expression {
  String toSQL();

  String toSQLValue();
}
