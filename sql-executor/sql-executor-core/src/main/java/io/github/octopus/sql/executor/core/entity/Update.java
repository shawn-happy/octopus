package io.github.octopus.sql.executor.core.entity;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Update {
  private String database;
  private String table;
  private Map<String, Object> updateParams;
  private String whereParams;
}
