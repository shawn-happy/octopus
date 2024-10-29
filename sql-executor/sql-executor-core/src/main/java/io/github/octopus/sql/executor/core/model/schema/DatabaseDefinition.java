package io.github.octopus.sql.executor.core.model.schema;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatabaseDefinition {
  private String database;
  private String charset;
  private String sortBy;
  private Map<String, Object> options;
}
