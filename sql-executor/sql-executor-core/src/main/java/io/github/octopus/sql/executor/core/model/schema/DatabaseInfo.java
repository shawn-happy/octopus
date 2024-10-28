package io.github.octopus.sql.executor.core.model.schema;

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
public class DatabaseInfo {
  private String name;
  // for mysql
  private String charsetName;
  // for mysql/sqlserver
  private String collationName;

  // for sqlserver
  private List<String> schemas;

  // for doris
  private Map<String, String> properties;
}
