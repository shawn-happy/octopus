package com.octopus.actus.connector.jdbc.model;

import com.octopus.actus.connector.jdbc.model.dialect.doris.DataModelInfo;
import com.octopus.actus.connector.jdbc.model.dialect.doris.DistributionInfo;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableInfo {
  private DatabaseInfo databaseInfo;
  private String name;
  private String comment;
  private List<ColumnInfo> columns;
  private List<IndexInfo> indexes;

  // for mysql
  private PrimaryKeyInfo primaryKeyInfo;

  // for doris
  private PartitionInfo partitionInfo;
  private DistributionInfo distributionInfo;
  private DataModelInfo dataModelInfo;
  private Map<String, String> tableOptions;
}
