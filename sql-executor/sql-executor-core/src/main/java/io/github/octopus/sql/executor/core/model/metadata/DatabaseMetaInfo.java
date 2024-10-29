package io.github.octopus.sql.executor.core.model.metadata;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatabaseMetaInfo {
  private String database;
  // for sqlserver
  private List<String> schemas;

  // for mysql
  private String charset;
  private String sortBy;
}
