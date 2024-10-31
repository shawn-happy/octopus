package io.github.octopus.sql.executor.plugin.api.dialect;

import com.baomidou.mybatisplus.annotation.DbType;
import io.github.octopus.sql.executor.core.exception.SqlException;
import io.github.octopus.sql.executor.core.model.FieldIdeEnum;
import io.github.octopus.sql.executor.core.model.schema.FieldType;
import io.github.octopus.sql.executor.core.model.schema.TableEngine;
import io.github.octopus.sql.executor.plugin.api.executor.AbstractCurdExecutor;
import io.github.octopus.sql.executor.plugin.api.executor.AbstractDDLExecutor;
import io.github.octopus.sql.executor.plugin.api.executor.AbstractMetaDataExecutor;
import java.util.List;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;

public interface JdbcDialect {

  String getDialectName();

  DbType getMybatisDbType();

  AbstractCurdExecutor createCurdExecutor(String name, DataSource dataSource);

  AbstractDDLExecutor createDDLExecutor(String name, DataSource dataSource);

  AbstractMetaDataExecutor createMetaDataExecutor(String name, DataSource dataSource);

  List<FieldType> getSupportedFieldTypes();

  String buildPageSql(String sql, long offset, long limit);

  default List<TableEngine> getSupportedTableEngines() {
    throw new UnsupportedOperationException(
        String.format("The [%s] does not support storage engines", getDialectName()));
  }

  default TableEngine toTableEngine(String tableEngine) {
    if (StringUtils.isBlank(tableEngine)) {
      return null;
    }
    List<TableEngine> supportedTableEngines = getSupportedTableEngines();
    return supportedTableEngines
        .stream()
        .filter(engine -> engine.getEngine().equalsIgnoreCase(tableEngine))
        .findFirst()
        .orElseThrow(
            () ->
                new SqlException(
                    String.format(
                        "The [%s] does not support the [%s] table engine",
                        getDialectName(), tableEngine)));
  }

  default FieldType toFieldType(String fieldType) {
    return getSupportedFieldTypes()
        .stream()
        .filter(type -> type.getDataType().equalsIgnoreCase(fieldType))
        .findFirst()
        .orElseThrow(
            () ->
                new SqlException(
                    String.format(
                        "field type [%s] not supported with jdbc type [%s]",
                        fieldType, getDialectName())));
  }

  default String quoteIdentifier(String identifier) {
    return identifier;
  }

  /** Quotes the identifier for database name or field name */
  default String quoteDatabaseIdentifier(String identifier) {
    return identifier;
  }

  default String tableIdentifier(String database, String tableName) {
    return quoteDatabaseIdentifier(database) + "." + quoteIdentifier(tableName);
  }

  default String getFieldIde(String identifier, String fieldIde) {
    if (StringUtils.isEmpty(fieldIde)) {
      return identifier;
    }
    switch (FieldIdeEnum.valueOf(fieldIde.toUpperCase())) {
      case LOWERCASE:
        return identifier.toLowerCase();
      case UPPERCASE:
        return identifier.toUpperCase();
      default:
        return identifier;
    }
  }

  String getUrl(String host, int port, String database, String suffix);
}
