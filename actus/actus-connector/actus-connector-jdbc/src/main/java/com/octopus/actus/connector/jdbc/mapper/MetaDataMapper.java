package com.octopus.actus.connector.jdbc.mapper;

import com.octopus.actus.connector.jdbc.DbType;
import com.octopus.actus.connector.jdbc.entity.ColumnMeta;
import com.octopus.actus.connector.jdbc.entity.DatabaseMeta;
import com.octopus.actus.connector.jdbc.entity.TableMeta;
import com.octopus.actus.connector.jdbc.model.ColumnKey;
import com.octopus.actus.connector.jdbc.model.ColumnMetaInfo;
import com.octopus.actus.connector.jdbc.model.DatabaseInfo;
import com.octopus.actus.connector.jdbc.model.FieldType;
import com.octopus.actus.connector.jdbc.model.TableEngine;
import com.octopus.actus.connector.jdbc.model.TableMetaInfo;
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
                    .name(databaseMeta.getSchemaName())
                    .charsetName(meta.getDefaultCharacterSetName())
                    .collationName(meta.getDefaultCollationName())
                    .build())
        .orElse(null);
  }

  public static List<DatabaseInfo> fromDatabaseMetas(List<DatabaseMeta> databaseMetas) {
    if (CollectionUtils.isEmpty(databaseMetas)) {
      return null;
    }
    return databaseMetas.stream()
        .map(MetaDataMapper::fromDatabaseMeta)
        .collect(Collectors.toList());
  }

  public static TableMetaInfo fromTableMeta(DbType dbType, TableMeta tableMeta) {
    return Optional.ofNullable(tableMeta)
        .map(
            meta ->
                TableMetaInfo.builder()
                    .databaseName(meta.getTableSchema())
                    .tableName(meta.getTableName())
                    .recordSize(meta.getTableRows())
                    .dataLength(meta.getDataLength())
                    .maxDataLength(meta.getMaxDataLength())
                    .comment(meta.getTableComment())
                    .engine(TableEngine.of(dbType, meta.getEngine()))
                    .createTime(meta.getCreateTime())
                    .updateTime(meta.getUpdateTime())
                    .collation(meta.getTableCollation())
                    .build())
        .orElse(null);
  }

  public static List<TableMetaInfo> fromTableMetas(DbType dbType, List<TableMeta> tableMetas) {
    if (CollectionUtils.isEmpty(tableMetas)) {
      return null;
    }
    return tableMetas.stream()
        .map(meta -> fromTableMeta(dbType, meta))
        .collect(Collectors.toList());
  }

  public static ColumnMetaInfo fromColumnMeta(DbType dbType, ColumnMeta columnMeta) {
    return Optional.ofNullable(columnMeta)
        .map(
            meta ->
                ColumnMetaInfo.builder()
                    .databaseName(meta.getTableSchema())
                    .tableName(meta.getTableName())
                    .columnName(meta.getColumnName())
                    .comment(meta.getColumnComment())
                    .nullable(StringUtils.equalsIgnoreCase("YES", meta.getIsNullable()))
                    .defaultValue(meta.getColumnDefault())
                    .fieldType(FieldType.of(dbType, meta.getDataType()))
                    .precision(
                        FieldType.of(dbType, meta.getDataType()).isString()
                            ? meta.getCharacterMaximumLength()
                            : (FieldType.of(dbType, meta.getDataType()).isNumeric()
                                ? meta.getNumericPrecision()
                                : meta.getDateTimePrecision()))
                    .scale(meta.getNumericScale())
                    .columnKey(ColumnKey.of(dbType, meta.getColumnKey()))
                    .build())
        .orElse(null);
  }

  public static List<ColumnMetaInfo> fromColumnMetas(DbType dbType, List<ColumnMeta> columnMetas) {
    if (CollectionUtils.isEmpty(columnMetas)) {
      return null;
    }
    return columnMetas.stream()
        .map(meta -> fromColumnMeta(dbType, meta))
        .collect(Collectors.toList());
  }
}
