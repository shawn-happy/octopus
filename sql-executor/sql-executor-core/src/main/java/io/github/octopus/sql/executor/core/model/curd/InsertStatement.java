package io.github.octopus.sql.executor.core.model.curd;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InsertStatement {
  private String database;
  // for sqlserver
  private String schema;
  private String table;
  private List<String> columns;
  private List<Object[]> values;
}
