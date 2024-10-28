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
public class DeleteStatement {
  private String database;
  private String table;
  private Expression expression;
}
