package io.github.octopus.datos.centro.sql.executor.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatabaseMeta {
  private String schemaName;
  private String defaultCharacterSetName;
  private String defaultCollationName;
}
