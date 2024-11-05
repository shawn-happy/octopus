package io.github.octopus.sql.executor.core.model.curd;

import io.github.octopus.sql.executor.core.model.expression.Expression;
import io.github.octopus.sql.executor.core.model.schema.TablePath;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RowExistsStatement {
    private TablePath tablePath;
    private Expression expression;
}
