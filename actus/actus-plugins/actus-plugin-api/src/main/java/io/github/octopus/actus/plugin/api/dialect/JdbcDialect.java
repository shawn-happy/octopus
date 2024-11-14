package io.github.octopus.actus.plugin.api.dialect;

import io.github.octopus.actus.core.exception.SqlException;
import io.github.octopus.actus.core.model.FieldIdeEnum;
import io.github.octopus.actus.core.model.schema.FieldType;
import io.github.octopus.actus.core.model.schema.TableEngine;
import io.github.octopus.actus.plugin.api.executor.CurdExecutor;
import io.github.octopus.actus.plugin.api.executor.DDLExecutor;
import io.github.octopus.actus.plugin.api.executor.MetaDataExecutor;
import java.util.List;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;

public interface JdbcDialect {

  String getDialectName();

  CurdExecutor createCurdExecutor(DataSource dataSource);

  DDLExecutor createDDLExecutor(DataSource dataSource);

  MetaDataExecutor createMetaDataExecutor(DataSource dataSource);

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

  default String quoteLeft() {
    return "`";
  }

  default String quoteRight() {
    return "`";
  }

  default FieldIdeEnum getFieldIde() {
    return FieldIdeEnum.ORIGINAL;
  }

  String getUrl(String host, int port, String database, String suffix);
}
