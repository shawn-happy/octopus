package io.github.octopus.actus.core.model.curd;

import io.github.octopus.actus.core.model.expression.Expression;
import io.github.octopus.actus.core.model.schema.TablePath;
import java.util.LinkedHashMap;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UpdateStatement {
  private TablePath tablePath;
  private LinkedHashMap<String, Object> updateParams = new LinkedHashMap<>();
  private Expression expression;
}
