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
                    .partitionDefinition(toPartitionEntity(info.getPartitions()))
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

  public static Partition toPartitionEntity(PartitionDefinition partitionInfo) {
    if (partitionInfo == null) {
      return null;
    }
    final PartitionOperator partitionOperator = partitionInfo.getPartitionOperator();
    final Partition.PartitionBuilder partitionBuilder =
        Partition.builder().columns(partitionInfo.getColumns());
    if (DorisPartitionOperator.LessThan.equals(partitionOperator)) {
      partitionBuilder
          .lessThan(
              DorisPartitionOperator.LessThan.equals(partitionInfo.getPartitionOperator())
                  || MySQLPartitionOperator.LessThan.equals(partitionInfo.getPartitionOperator()))
          .lessThanPartitions(toLessThanPartitionEntities(partitionInfo));
    } else if (DorisPartitionOperator.FIXED_RANGE.equals(partitionOperator)) {
      partitionBuilder
          .fixedRange(
              DorisPartitionOperator.FIXED_RANGE.equals(partitionInfo.getPartitionOperator()))
          .fixedRangePartitions(toFixedRangePartitionEntities(partitionInfo));
    } else if (DorisPartitionOperator.DATE_MULTI_RANGE.equals(partitionOperator)) {
      partitionBuilder
          .dateMultiRange(
              DorisPartitionOperator.DATE_MULTI_RANGE.equals(partitionInfo.getPartitionOperator()))
          .dateMultiRangePartitions(toDateMultiRangePartitionEntities(partitionInfo));
    } else if (DorisPartitionOperator.NUMERIC_MULTI_RANGE.equals(partitionOperator)) {
      partitionBuilder
          .numericMultiRange(
              DorisPartitionOperator.NUMERIC_MULTI_RANGE.equals(
                  partitionInfo.getPartitionOperator()))
          .numericMultiRangePartitions(toNumericMultiRangePartitionEntities(partitionInfo));
    }
    return partitionBuilder.build();
  }

  public static List<LessThanPartition> toLessThanPartitionEntities(
      PartitionDefinition partitionInfo) {
    List<String> columns = partitionInfo.getColumns();
    List<String> partitionNames = partitionInfo.getNames();
    List<Object[]> maxValues = partitionInfo.getMaxValues();
    if (CollectionUtils.isEmpty(columns)
        || CollectionUtils.isEmpty(partitionNames)
        || CollectionUtils.isEmpty(maxValues)) {
      throw new NullPointerException(
          "partition columns & names & maxValues can not be null or empty");
    }
    if (maxValues.size() != partitionNames.size()) {
      throw new IllegalStateException("less_than partition size more than num of maxValues");
    }
    for (Object[] maxValue : maxValues) {
      if (maxValue.length != columns.size()) {
        throw new IllegalStateException(
            "less_than partition column size more than length of maxValue");
      }
    }
    List<LessThanPartition> partitions = new ArrayList<>(partitionNames.size());
    for (int i = 0; i < partitionNames.size(); i++) {
      String partitionName = partitionNames.get(i);
      Object[] maxValue = maxValues.get(i);
      partitions.add(toLessThanPartitionEntity(partitionName, maxValue));
    }
    return partitions;
  }

  public static LessThanPartition toLessThanPartitionEntity(String name, Object[] maxValues) {
    return LessThanPartition.builder().partitionName(name).maxValues(maxValues).build();
  }

  public static List<DateMultiRangePartition> toDateMultiRangePartitionEntities(
      PartitionDefinition partitionInfo) {
    List<String> columns = partitionInfo.getColumns();
    List<Object[]> lefts = partitionInfo.getLefts();
    List<Object[]> rights = partitionInfo.getRights();
    if (CollectionUtils.isEmpty(columns)
        || CollectionUtils.isEmpty(rights)
        || CollectionUtils.isEmpty(lefts)) {
      throw new NullPointerException(
          "date multi range partition columns & lefts & rights can not be null or empty");
    }
    if (lefts.size() != rights.size()) {
      throw new IllegalStateException("date multi range partition from size must equals to size");
    }
    for (Object[] left : lefts) {
      if (left.length != columns.size()) {
        throw new IllegalStateException(
            "date multi range partition column size more than length of left");
      }
    }

    for (Object[] right : rights) {
      if (right.length != columns.size()) {
        throw new IllegalStateException(
            "date multi range partition size more than length of right");
      }
    }
    List<DateMultiRangePartition> partitions = new ArrayList<>(lefts.size());
    for (int i = 0; i < lefts.size(); i++) {
      Object[] left = lefts.get(i);
      Object[] right = rights.get(i);
      partitions.add(
          toDateMultiRangePartitionEntity(
              ArrayUtils.toStringArray(left),
              ArrayUtils.toStringArray(right),
              partitionInfo.getInterval()[i],
              partitionInfo.getIntervalType()[i]));
    }
    return partitions;
  }

  public static DateMultiRangePartition toDateMultiRangePartitionEntity(
      String[] from, String[] to, long interval, IntervalType intervalType) {
    return DateMultiRangePartition.builder()
        .from(from)
        .to(to)
        .interval(interval)
        .type(intervalType.name())
        .build();
  }

  public static List<FixedRangePartition> toFixedRangePartitionEntities(
      PartitionDefinition partitionInfo) {
    List<String> columns = partitionInfo.getColumns();
    List<String> partitionNames = partitionInfo.getNames();
    List<Object[]> lefts = partitionInfo.getLefts();
    List<Object[]> rights = partitionInfo.getRights();
    if (CollectionUtils.isEmpty(columns)
        || CollectionUtils.isEmpty(partitionNames)
        || CollectionUtils.isEmpty(rights)
        || CollectionUtils.isEmpty(lefts)) {
      throw new NullPointerException(
          "fixed range partition columns & names & lefts & rights can not be null or empty");
    }
    if (lefts.size() != partitionNames.size() || rights.size() != partitionNames.size()) {
      throw new IllegalStateException(
          "fixed_range partition size more than num of lefts or rights");
    }
    for (Object[] left : lefts) {
      if (left.length != columns.size()) {
        throw new IllegalStateException(
            "fixed_range partition column size more than length of left");
      }
    }

    for (Object[] right : rights) {
      if (right.length != columns.size()) {
        throw new IllegalStateException(
            "fixed_range partition column size more than length of right");
      }
    }
    List<FixedRangePartition> partitions = new ArrayList<>(partitionNames.size());
    for (int i = 0; i < partitionNames.size(); i++) {
      String partitionName = partitionNames.get(i);
      Object[] left = lefts.get(i);
      Object[] right = rights.get(i);
      partitions.add(toFixedRangePartitionEntity(partitionName, left, right));
    }
    return partitions;
  }

  public static FixedRangePartition toFixedRangePartitionEntity(
      String name, Object[] left, Object[] right) {
    return FixedRangePartition.builder().partitionName(name).lefts(left).rights(right).build();
  }

  public static List<NumericMultiRangePartition> toNumericMultiRangePartitionEntities(
      PartitionDefinition partitionInfo) {
    List<String> columns = partitionInfo.getColumns();
    List<Object[]> lefts = partitionInfo.getLefts();
    List<Object[]> rights = partitionInfo.getRights();
    if (CollectionUtils.isEmpty(columns)
        || CollectionUtils.isEmpty(rights)
        || CollectionUtils.isEmpty(lefts)) {
      throw new NullPointerException(
          "numeric multi range partition columns & lefts & rights can not be null or empty");
    }
    if (lefts.size() != rights.size()) {
      throw new IllegalStateException(
          "numeric multi range partition from size must equals to size");
    }
    for (Object[] left : lefts) {
      if (left.length < columns.size()) {
        throw new IllegalStateException(
            "numeric multi range partition column size more than length of left");
      }
    }

    for (Object[] right : rights) {
      if (right.length < columns.size()) {
        throw new IllegalStateException(
            "numeric multi range partition size more than length of right");
      }
    }
    List<NumericMultiRangePartition> partitions = new ArrayList<>(lefts.size());
    for (int i = 0; i < lefts.size(); i++) {
      Long[] left = (Long[]) lefts.get(i);
      Long[] right = (Long[]) rights.get(i);
      partitions.add(
          toNumericMultiRangePartitionEntity(
              ArrayUtils.toPrimitive(left),
              ArrayUtils.toPrimitive(right),
              partitionInfo.getInterval()[i]));
    }
    return partitions;
  }

  public static NumericMultiRangePartition toNumericMultiRangePartitionEntity(
      long[] left, long[] right, long interval) {
    return NumericMultiRangePartition.builder().from(left).to(right).interval(interval).build();
  }

  public static Distribution toDistributionEntity(TabletDefinition distributionInfo) {
    return Optional.ofNullable(distributionInfo)
        .map(
            info ->
                Distribution.builder()
                    .columns(info.getColumns())
                    .algo(info.getAlgo().getAlgo())
                    .num(info.getNum())
                    .build())
        .orElse(null);
  }

  public static DataModelKey toDataModelEntity(AggregateModelDefinition dataModelInfo) {
    return Optional.ofNullable(dataModelInfo)
        .map(
            info ->
                DataModelKey.builder()
                    .key(info.getType().getType())
                    .columns(info.getColumns())
                    .build())
        .orElse(null);
  }
}
