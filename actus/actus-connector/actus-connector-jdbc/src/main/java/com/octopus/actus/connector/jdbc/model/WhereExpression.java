package com.octopus.actus.connector.jdbc.model;

public interface WhereExpression {
  String toSQL();

  String toSQLValue();
}
