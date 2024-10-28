package io.github.octopus.sql.executor.plugin.api.mapper;

import io.github.octopus.sql.executor.core.entity.ColumnMeta;
import io.github.octopus.sql.executor.core.entity.DatabaseMeta;
import io.github.octopus.sql.executor.core.entity.TableMeta;
import io.github.octopus.sql.executor.core.model.metadata.ColumnMetaInfo;
import io.github.octopus.sql.executor.core.model.metadata.TableMetaInfo;
import io.github.octopus.sql.executor.core.model.schema.DatabaseInfo;
import io.github.octopus.sql.executor.plugin.api.dialect.DialectRegistry;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcType;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class MetaDataMapper {

  public static DatabaseInfo fromDatabaseMeta(DatabaseMeta databaseMeta) {
    return Optional.ofNullable(databaseMeta)
        .map(
            meta ->
                DatabaseInfo.builder()
                    .name(databaseMeta.getName())
                    .charsetName(meta.getCharacterSet())
                    .collationName(meta.getCollation())
                    .build())
        .orElse(null);
  }

  public static List<DatabaseInfo> fromDatabaseMetas(List<DatabaseMeta> databaseMetas) {
    if (CollectionUtils.isEmpty(databaseMetas)) {
      return null;
    }
    return databaseMetas
        .stream()
        .map(MetaDataMapper::fromDatabaseMeta)
        .collect(Collectors.toList());
  }

  public static TableMetaInfo fromTableMeta(JdbcType dbType, TableMeta tableMeta) {
    JdbcDialect dialect = DialectRegistry.getDialect(dbType.getType());
    return Optional.ofNullable(tableMeta)
        .map(
            meta ->
                TableMetaInfo.builder()
                    .databaseName(meta.getDatabase())
                    .schemaName(meta.getSchema())
                    .tableName(meta.getTable())
                    .recordSize(meta.getRowSize())
                    .recordNumber(meta.getRowNumber())
                    .comment(meta.getComment())
                    .engine(dialect.toTableEngine(tableMeta.getEngine()))
                    .createTime(meta.getCreateTime())
                    .updateTime(meta.getUpdateTime())
                    .build())
        .orElse(null);
  }

  public static List<TableMetaInfo> fromTableMetas(JdbcType dbType, List<TableMeta> tableMetas) {
    if (CollectionUtils.isEmpty(tableMetas)) {
      return null;
    }
    return tableMetas
        .stream()
        .map(meta -> fromTableMeta(dbType, meta))
        .collect(Collectors.toList());
  }

  public static ColumnMetaInfo fromColumnMeta(JdbcType dbType, ColumnMeta columnMeta) {
    JdbcDialect dialect = DialectRegistry.getDialect(dbType.getType());
    return Optional.ofNullable(columnMeta)
        .map(
            meta ->
                ColumnMetaInfo.builder()
                    .databaseName(meta.getDatabase())
                    .schemaName(meta.getSchema())
                    .tableName(meta.getTable())
                    .columnName(meta.getColumn())
                    .comment(meta.getComment())
                    .nullable(StringUtils.equalsIgnoreCase("YES", meta.getNullable()))
                    .defaultValue(meta.getDefaultValue())
                    .fieldType(dialect.toFieldType(meta.getDataType()))
                    .precision(
                        dialect.toFieldType(meta.getDataType()).isString()
                            ? meta.getLength().intValue()
                            : (dialect.toFieldType(meta.getDataType()).isNumeric()
                                ? meta.getPrecision()
                                : meta.getTimePrecision()))
                    .scale(meta.getScale())
                    .build())
        .orElse(null);
  }

  public static List<ColumnMetaInfo> fromColumnMetas(
      JdbcType dbType, List<ColumnMeta> columnMetas) {
    if (CollectionUtils.isEmpty(columnMetas)) {
      return null;
    }
    return columnMetas
        .stream()
        .map(meta -> fromColumnMeta(dbType, meta))
        .collect(Collectors.toList());
  }
}
