package io.github.octopus.sql.executor.core.model.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableInfo {
  private DatabaseInfo databaseInfo;
  private String schema;
  private String name;
  private String comment;
  private List<ColumnInfo> columns;
  private List<IndexInfo> indexes = new ArrayList<>();

  private PrimaryKeyInfo primaryKeyInfo;
  private List<ConstraintInfo> constraints;

  // for doris
  private PartitionInfo partitionInfo;
  private Map<String, String> tableOptions;

  public void addIndex(IndexInfo indexInfo) {
    if (indexInfo != null) {
      this.indexes.add(indexInfo);
    }
  }

  public void addIndexes(Collection<IndexInfo> indexes) {
    if (CollectionUtils.isEmpty(indexes)) {
      return;
    }
    indexes.forEach(this::addIndex);
  }

  public PrimaryKeyInfo getPrimaryKeyInfo() {
    if (primaryKeyInfo == null) {
      List<ColumnInfo> pkColumns =
          columns.stream().filter(ColumnInfo::isPrimaryKey).collect(Collectors.toList());
      if (CollectionUtils.isEmpty(pkColumns)) {
        return null;
      }
      primaryKeyInfo =
          PrimaryKeyInfo.builder()
              .columns(pkColumns.stream().map(ColumnInfo::getName).collect(Collectors.toList()))
              .name(
                  String.format(
                      "%s_%s_%s_pk",
                      databaseInfo.getName(),
                      name,
                      pkColumns.stream().map(ColumnInfo::getName).collect(Collectors.joining("_"))))
              .build();
    }
    if (primaryKeyInfo == null && CollectionUtils.isNotEmpty(constraints)) {
      constraints
          .stream()
          .filter(
              constraintInfo -> constraintInfo.getConstraintType() == ConstraintType.PRIMARY_KEY)
          .findFirst()
          .ifPresent(
              constraintInfo ->
                  primaryKeyInfo =
                      PrimaryKeyInfo.builder()
                          .columns(constraintInfo.getConstraints())
                          .name(constraintInfo.getConstraintName())
                          .build());
    }

    return primaryKeyInfo;
  }
}
