package io.github.octopus.sql.executor.core.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ConstraintKeyMeta {
  private String name;
  private String type;
  private String column;
  private String database;
  private String schema;
  private String table;
}
