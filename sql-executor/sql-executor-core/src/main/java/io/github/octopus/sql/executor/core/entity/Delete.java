package io.github.octopus.sql.executor.core.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Delete {
  private String database;
  private String table;
  private String whereParams;
}
