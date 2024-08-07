package io.github.octopus.datos.centro.sql.executor.mapper;

import io.github.octopus.datos.centro.sql.executor.entity.Column;
import io.github.octopus.datos.centro.sql.executor.entity.DataModelKey;
import io.github.octopus.datos.centro.sql.executor.entity.Distribution;
import io.github.octopus.datos.centro.sql.executor.entity.Index;
import io.github.octopus.datos.centro.sql.executor.entity.Partition;
import io.github.octopus.datos.centro.sql.executor.entity.Partition.DateMultiRangePartition;
import io.github.octopus.datos.centro.sql.executor.entity.Partition.FixedRangePartition;
import io.github.octopus.datos.centro.sql.executor.entity.Partition.LessThanPartition;
import io.github.octopus.datos.centro.sql.executor.entity.Partition.NumericMultiRangePartition;
import io.github.octopus.datos.centro.sql.executor.entity.PrimaryKey;
import io.github.octopus.datos.centro.sql.executor.entity.Table;
import io.github.octopus.datos.centro.sql.model.ColumnInfo;
import io.github.octopus.datos.centro.sql.model.FieldType;
import io.github.octopus.datos.centro.sql.model.IndexAlgo;
import io.github.octopus.datos.centro.sql.model.IndexInfo;
import io.github.octopus.datos.centro.sql.model.IntervalType;
import io.github.octopus.datos.centro.sql.model.PartitionInfo;
import io.github.octopus.datos.centro.sql.model.PartitionOperator;
import io.github.octopus.datos.centro.sql.model.PrimaryKeyInfo;
import io.github.octopus.datos.centro.sql.model.TableInfo;
import io.github.octopus.datos.centro.sql.model.dialect.doris.AggregateAlgo;
import io.github.octopus.datos.centro.sql.model.dialect.doris.DataModelInfo;
import io.github.octopus.datos.centro.sql.model.dialect.doris.DistributionInfo;
import io.github.octopus.datos.centro.sql.model.dialect.doris.DorisPartitionOperator;
import io.github.octopus.datos.centro.sql.model.dialect.mysql.MySQLPartitionOperator;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.jetbrains.annotations.NotNull;

public class DDLMapper {

  private DDLMapper() {}

  public static Table toTableEntity(TableInfo tableInfo) {
    return Optional.ofNullable(tableInfo)
        .map(
            info ->
                Table.builder()
                    .databaseName(info.getDatabaseInfo().getName())
                    .tableName(info.getName())
                    .columnDefinitions(toColumnEntities(info.getColumns()))
                    .indexDefinitions(toIndexEntities(info.getIndexes()))
                    .comment(info.getComment())
                    .primaryKey(toPKEntity(info.getPrimaryKeyInfo()))
                    .partitionDefinition(toPartitionEntity(info.getPartitionInfo()))
                    .distributionDefinition(toDistributionEntity(info.getDistributionInfo()))
                    .keyDefinition(toDataModelEntity(info.getDataModelInfo()))
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
                    .aggregateType(
                        Optional.ofNullable(info.getAggregateAlgo())
                            .map(AggregateAlgo::getAlgo)
                            .orElse(null))
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

  public static List<LessThanPartition> toLessThanPartitionEntities(PartitionInfo partitionInfo) {
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
      PartitionInfo partitionInfo) {
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
      PartitionInfo partitionInfo) {
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
      PartitionInfo partitionInfo) {
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

  public static Distribution toDistributionEntity(DistributionInfo distributionInfo) {
    return Optional.ofNullable(distributionInfo)
        .map(
            info ->
                Distribution.builder()
                    .columns(info.getColumns())
                    .algo(info.getDistributionAlgo().getAlgo())
                    .num(info.getNum())
                    .build())
        .orElse(null);
  }

  public static DataModelKey toDataModelEntity(DataModelInfo dataModelInfo) {
    return Optional.ofNullable(dataModelInfo)
        .map(
            info ->
                DataModelKey.builder()
                    .key(dataModelInfo.getAggregateType().getKey())
                    .columns(dataModelInfo.getColumns())
                    .build())
        .orElse(null);
  }
}
