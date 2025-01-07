package io.github.octopus.actus.plugin.api.dialect;

import io.github.octopus.actus.core.model.schema.TablePath;

public interface SqlStatement {
  JdbcDialect getJdbcDialect();

  default String tableIdentifier(TablePath tablePath) {
    return tablePath.getFullNameWithQuoted(
        getJdbcDialect().quoteLeft(),
        getJdbcDialect().quoteRight(),
        getJdbcDialect().getFieldIde());
  }

  default String databaseIdentifier(TablePath tablePath) {
    return tablePath.getDatabaseNameWithQuoted(
        getJdbcDialect().quoteLeft(),
        getJdbcDialect().quoteRight(),
        getJdbcDialect().getFieldIde());
  }

  default String schemaIdentifier(TablePath tablePath) {
    return tablePath.getSchemaNameWithQuoted(
        getJdbcDialect().quoteLeft(),
        getJdbcDialect().quoteRight(),
        getJdbcDialect().getFieldIde());
  }

  default String quoteIdentifier(String identifier) {
    return getJdbcDialect().quoteLeft()
        + getJdbcDialect().getFieldIde().identifier(identifier)
        + getJdbcDialect().quoteRight();
  }
}
