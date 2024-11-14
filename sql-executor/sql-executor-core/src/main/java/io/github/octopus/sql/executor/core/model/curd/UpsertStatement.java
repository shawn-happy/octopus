package io.github.octopus.sql.executor.core.model.curd;

import io.github.octopus.sql.executor.core.model.schema.ColumnDefinition;
import io.github.octopus.sql.executor.core.model.schema.ConstraintDefinition;
import io.github.octopus.sql.executor.core.model.schema.ConstraintType;
import io.github.octopus.sql.executor.core.model.schema.TablePath;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UpsertStatement {
  private TablePath tablePath;
  private List<ColumnDefinition> columns;
  private List<ConstraintDefinition> constraints;
  private List<Object[]> values;

  public List<String> fullColumns() {
    return columns.stream().map(ColumnDefinition::getColumn).collect(Collectors.toList());
  }

  public List<String> uniqueColumns() {
    if (CollectionUtils.isNotEmpty(constraints)) {
      return constraints
          .stream()
          .filter(
              constraint ->
                  constraint.getConstraintType() == ConstraintType.PRIMARY_KEY
                      || constraint.getConstraintType() == ConstraintType.UNIQUE_KEY)
          .flatMap(constraint -> constraint.getColumns().stream())
          .distinct()
          .collect(Collectors.toList());
    }
    return null;
  }

  public List<String> nonUniqueColumns() {
    List<String> uniqueColumns = uniqueColumns();
    if (CollectionUtils.isEmpty(uniqueColumns)) {
      return fullColumns();
    }
    return columns
        .stream()
        .map(ColumnDefinition::getColumn)
        .filter(col -> !uniqueColumns.contains(col))
        .collect(Collectors.toList());
  }
}
