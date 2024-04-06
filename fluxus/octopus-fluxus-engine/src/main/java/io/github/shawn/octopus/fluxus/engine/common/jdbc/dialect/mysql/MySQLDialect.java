package io.github.shawn.octopus.fluxus.engine.common.jdbc.dialect.mysql;

import io.github.shawn.octopus.fluxus.engine.common.Constants;
import io.github.shawn.octopus.fluxus.engine.common.jdbc.dialect.JdbcDialect;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class MySQLDialect implements JdbcDialect {
  @Override
  public String dialectName() {
    return Constants.JdbcConstant.MYSQL;
  }

  @Override
  public String quoteIdentifier(String identifier) {
    return String.format("`%s`", identifier);
  }

  @Override
  public MySQLDialectTypeMapper getJdbcDialectTypeMapper() {
    return new MySQLDialectTypeMapper();
  }

  @Override
  public Optional<String> getUpsertStatement(
      String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
    String updateClause =
        Arrays.stream(fieldNames)
            .map(
                fieldName ->
                    quoteIdentifier(fieldName) + "=VALUES(" + quoteIdentifier(fieldName) + ")")
            .collect(Collectors.joining(", "));
    String upsertSQL =
        getInsertIntoStatement(database, tableName, fieldNames)
            + " ON DUPLICATE KEY UPDATE "
            + updateClause;
    return Optional.of(upsertSQL);
  }

  public static class MySQLDialectTypeMapper implements JdbcDialectTypeMapper {}
}
