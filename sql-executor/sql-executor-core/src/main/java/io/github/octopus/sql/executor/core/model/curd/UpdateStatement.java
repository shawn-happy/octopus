package io.github.octopus.sql.executor.core.model.curd;

import io.github.octopus.sql.executor.core.model.expression.Expression;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UpdateStatement {
  private String database;
  private String table;
  private UpdateParams updateParams;
  private Expression expression;
}
