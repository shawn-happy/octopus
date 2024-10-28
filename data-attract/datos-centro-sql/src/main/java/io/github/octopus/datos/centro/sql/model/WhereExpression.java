package io.github.octopus.datos.centro.sql.model;

public interface WhereExpression {
  String toSQL();

  String toSQLValue();
}
