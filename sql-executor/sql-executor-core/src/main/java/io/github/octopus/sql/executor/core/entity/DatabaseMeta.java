package io.github.octopus.sql.executor.core.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatabaseMeta {
  private String name;
  private String characterSet;
  private String collation;
}
