package io.github.octopus.actus.core.handler;

import io.github.octopus.actus.core.model.schema.FieldType;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface TypeHandler<T> {
  void setParameter(PreparedStatement ps, int i, T parameter, FieldType fieldType)
      throws SQLException;

  /**
   * Gets the result.
   *
   * @param rs the rs
   * @param columnName Column name, when configuration <code>useColumnLabel</code> is <code>false
   *     </code>
   * @return the result
   * @throws SQLException the SQL exception
   */
  T getResult(ResultSet rs, String columnName) throws SQLException;

  T getResult(ResultSet rs, int columnIndex) throws SQLException;

  T getResult(CallableStatement cs, int columnIndex) throws SQLException;
}
