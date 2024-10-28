package io.github.octopus.sql.executor.plugin.api.mapper;

import io.github.octopus.sql.executor.core.entity.Column;
import io.github.octopus.sql.executor.core.entity.Constraint;
import io.github.octopus.sql.executor.core.entity.Index;
import io.github.octopus.sql.executor.core.entity.Partition;
import io.github.octopus.sql.executor.core.entity.PrimaryKey;
import io.github.octopus.sql.executor.core.entity.Table;
import io.github.octopus.sql.executor.core.model.schema.ColumnInfo;
import io.github.octopus.sql.executor.core.model.schema.ConstraintInfo;
import io.github.octopus.sql.executor.core.model.schema.DatabaseInfo;
import io.github.octopus.sql.executor.core.model.schema.FieldType;
import io.github.octopus.sql.executor.core.model.schema.IndexAlgo;
import io.github.octopus.sql.executor.core.model.schema.IndexInfo;
import io.github.octopus.sql.executor.core.model.schema.PartitionInfo;
import io.github.octopus.sql.executor.core.model.schema.PartitionOperator;
import io.github.octopus.sql.executor.core.model.schema.PrimaryKeyInfo;
import io.github.octopus.sql.executor.core.model.schema.TableInfo;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.jetbrains.annotations.NotNull;

public class DDLMapper {

  private DDLMapper() {}

  public static Table toTableEntity(TableInfo tableInfo) {
    return Optional.ofNullable(tableInfo)
        .map(
            info ->
                Table.builder()
                    .databaseName(
                        Optional.ofNullable(info.getDatabaseInfo())
                            .map(DatabaseInfo::getName)
                            .orElse(null))
                    .schemaName(info.getSchema())
                    .tableName(info.getName())
                    .columnDefinitions(toColumnEntities(info.getColumns()))
                    .indexDefinitions(toIndexEntities(info.getIndexes()))
                    .constraintDefinitions(toConstraintEntities(info.getConstraints()))
                    .comment(info.getComment())
                    .primaryKey(toPKEntity(info.getPrimaryKeyInfo()))
                    .partitionDefinition(toPartitionEntity(info.getPartitionInfo()))
                    .tableProperties(info.getTableOptions())
                    .build())
        .orElse(null);
  }

  public static List<Column> toColumnEntities(List<ColumnInfo> columnInfos) {
    if (CollectionUtils.isEmpty(columnInfos)) {
      return null;
    }
    return columnInfos.stream().map(DDLMapper::toColumnEntity).collect(Collectors.toList());
  }

  public static Column toColumnEntity(ColumnInfo columnInfo) {
    return Optional.ofNullable(columnInfo)
        .map(
            info ->
                Column.builder()
                    .columnName(info.getName())
                    .fieldType(
                        toFieldTypeString(
                            info.getFieldType(), info.getPrecision(), info.getScale()))
                    .nullable(info.isNullable())
                    //                    .aggregateType(
                    //                        Optional.ofNullable(info.getAggregateAlgo())
                    //                            .map(AggregateAlgo::getAlgo)
                    //                            .orElse(null))
                    .autoIncrement(info.isAutoIncrement())
                    .defaultValue(info.getDefaultValue())
                    .comment(info.getComment())
                    .build())
        .orElse(null);
  }

  public static String toFieldTypeString(
      @NotNull FieldType fieldType, Integer precision, Integer scale) {
    String dataType = fieldType.getDataType();
    if (precision != null && scale != null) {
      return String.format("%s(%d,%d)", dataType, precision, scale);
    }
    if (precision != null) {
      return String.format("%s(%d)", dataType, precision);
    }
    return dataType;
  }

  public static List<Constraint> toConstraintEntities(List<ConstraintInfo> constraintInfos) {
    if (CollectionUtils.isEmpty(constraintInfos)) {
      return null;
    }
    return constraintInfos.stream().map(DDLMapper::toConstraintEntity).collect(Collectors.toList());
  }

  public static Constraint toConstraintEntity(ConstraintInfo constraintInfo) {
    return Optional.ofNullable(constraintInfo)
        .map(
            info ->
                Constraint.builder()
                    .name(info.getConstraintName())
                    .type(info.getConstraintType().getType())
                    .constraints(info.getConstraints())
                    .build())
        .orElse(null);
  }

  public static List<Index> toIndexEntities(List<IndexInfo> indexInfos) {
    if (CollectionUtils.isEmpty(indexInfos)) {
      return null;
    }
    return indexInfos.stream().map(DDLMapper::toIndexEntity).collect(Collectors.toList());
  }

  public static Index toIndexEntity(IndexInfo indexInfo) {
    return Optional.ofNullable(indexInfo)
        .map(
            info ->
                Index.builder()
                    .indexName(info.getName())
                    .algo(
                        Optional.ofNullable(info.getIndexAlgo())
                            .map(IndexAlgo::getAlgo)
                            .orElse(null))
                    .columns(info.getColumns())
                    .comment(info.getComment())
                    .build())
        .orElse(null);
  }

  public static PrimaryKey toPKEntity(PrimaryKeyInfo primaryKeyInfo) {
    return Optional.ofNullable(primaryKeyInfo)
        .map(
            info ->
                PrimaryKey.builder()
                    .primaryKeyName(info.getName())
                    .columns(info.getColumns())
                    .build())
        .orElse(null);
  }

  public static Partition toPartitionEntity(PartitionInfo partitionInfo) {
    if (partitionInfo == null) {
      return null;
    }
    final PartitionOperator partitionOperator = partitionInfo.getPartitionOperator();
    final Partition.PartitionBuilder partitionBuilder =
        Partition.builder().columns(partitionInfo.getColumns());
    return partitionBuilder.build();
  }
}
