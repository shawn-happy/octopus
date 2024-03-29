package com.octopus.actus.connector.jdbc.entity;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Table {
  private String databaseName;
  @NotNull private String tableName;
  private List<Column> columnDefinitions;
  private List<Index> indexDefinitions;
  private String comment;
  private Partition partitionDefinition;
  private Distribution distributionDefinition;
  private PrimaryKey primaryKey;
  private DataModelKey keyDefinition;
  private Map<String, String> tableProperties;
}
