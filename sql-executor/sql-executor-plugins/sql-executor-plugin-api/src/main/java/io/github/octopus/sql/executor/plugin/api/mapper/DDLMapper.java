package io.github.octopus.sql.executor.plugin.api.mapper;

import io.github.octopus.sql.executor.core.entity.Column;
import io.github.octopus.sql.executor.core.entity.Constraint;
import io.github.octopus.sql.executor.core.entity.DataModelKey;
import io.github.octopus.sql.executor.core.entity.Distribution;
import io.github.octopus.sql.executor.core.entity.Index;
import io.github.octopus.sql.executor.core.entity.Partition;
import io.github.octopus.sql.executor.core.entity.Partition.DateMultiRangePartition;
import io.github.octopus.sql.executor.core.entity.Partition.FixedRangePartition;
import io.github.octopus.sql.executor.core.entity.Partition.LessThanPartition;
import io.github.octopus.sql.executor.core.entity.Partition.NumericMultiRangePartition;
import io.github.octopus.sql.executor.core.entity.Table;
import io.github.octopus.sql.executor.core.model.schema.AggregateModelDefinition;
import io.github.octopus.sql.executor.core.model.schema.ColumnDefinition;
import io.github.octopus.sql.executor.core.model.schema.ConstraintDefinition;
import io.github.octopus.sql.executor.core.model.schema.FieldType;
import io.github.octopus.sql.executor.core.model.schema.IndexAlgo;
import io.github.octopus.sql.executor.core.model.schema.IndexDefinition;
import io.github.octopus.sql.executor.core.model.schema.IntervalType;
import io.github.octopus.sql.executor.core.model.schema.PartitionDefinition;
import io.github.octopus.sql.executor.core.model.schema.PartitionOperator;
import io.github.octopus.sql.executor.core.model.schema.TableDefinition;
import io.github.octopus.sql.executor.core.model.schema.TabletDefinition;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.jetbrains.annotations.NotNull;

public class DDLMapper {

  private DDLMapper() {}

  public static Table toTableEntity(TableDefinition tableInfo) {
    return Optional.ofNullable(tableInfo)
        .map(
            info ->
                Table.builder()
                    .databaseName(info.getDatabase())
                    .schemaName(info.getSchema())
                    .tableName(info.getTable())
                    .columnDefinitions(toColumnEntities(info.getColumns()))
                    .indexDefinitions(toIndexEntities(info.getIndices()))
                    .constraintDefinitions(toConstraintEntities(info.getConstraints()))
                    .comment(info.getComment())
                    .partitionDefinition(null)
                    .tableProperties(info.getOptions())
                    .build())
        .orElse(null);
  }

  public static List<Column> toColumnEntities(List<ColumnDefinition> columnInfos) {
    if (CollectionUtils.isEmpty(columnInfos)) {
      return null;
    }
    return columnInfos.stream().map(DDLMapper::toColumnEntity).collect(Collectors.toList());
  }

  public static Column toColumnEntity(ColumnDefinition columnInfo) {
    return Optional.ofNullable(columnInfo)
        .map(
            info ->
                Column.builder()
                    .columnName(info.getColumn())
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

  public static List<Constraint> toConstraintEntities(List<ConstraintDefinition> constraintInfos) {
    if (CollectionUtils.isEmpty(constraintInfos)) {
      return null;
    }
    return constraintInfos.stream().map(DDLMapper::toConstraintEntity).collect(Collectors.toList());
  }

  public static Constraint toConstraintEntity(ConstraintDefinition constraintInfo) {
    return Optional.ofNullable(constraintInfo)
        .map(
            info ->
                Constraint.builder()
                    .name(info.getName())
                    .type(info.getConstraintType().getType())
                    .constraints(info.getConstraints())
                    .build())
        .orElse(null);
  }

  public static List<Index> toIndexEntities(List<IndexDefinition> indexInfos) {
    if (CollectionUtils.isEmpty(indexInfos)) {
      return null;
    }
    return indexInfos.stream().map(DDLMapper::toIndexEntity).collect(Collectors.toList());
  }

  public static Index toIndexEntity(IndexDefinition indexInfo) {
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


}
