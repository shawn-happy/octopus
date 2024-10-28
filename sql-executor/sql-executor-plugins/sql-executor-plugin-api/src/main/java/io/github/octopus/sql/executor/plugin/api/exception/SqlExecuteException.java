package io.github.octopus.sql.executor.plugin.api.exception;

public class SqlExecuteException extends RuntimeException {
  public SqlExecuteException() {
    this("sql execute error...");
  }

  public SqlExecuteException(String sql) {
    super(String.format("sql execute error... \nsql: \n%s", sql));
  }

  public SqlExecuteException(String sql, Throwable cause) {
    super(String.format("sql execute error... \nsql: \n%s", sql), cause);
  }

  public SqlExecuteException(Throwable cause) {
    super(cause);
  }
}
