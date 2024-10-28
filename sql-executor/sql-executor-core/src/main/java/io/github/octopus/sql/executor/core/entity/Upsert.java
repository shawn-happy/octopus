package io.github.octopus.sql.executor.core.entity;

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
public class Upsert {
  private String database;
  private String table;
  private List<String> columns;
  private Map<String, Object> params;
  private List<String> uniqueColumns;
  private List<String> nonUniqueColumns;
}
