package io.github.octopus.sql.executor.core.model.schema;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TableDefinition {
  private String database;
  private String schema;
  private String table;
  private String comment;
  private List<ColumnDefinition> columns;
  private List<IndexDefinition> indices;
  private List<ConstraintDefinition> constraints;
  private List<PartitionDefinition> partitions;

  // for doris
  private TabletDefinition tablet;
  private AggregateModelDefinition aggregateModel;

  // for doris/mysql
  private TableEngine engine;

  // for doris 高级参数
  private Map<String, Object> options;
}
