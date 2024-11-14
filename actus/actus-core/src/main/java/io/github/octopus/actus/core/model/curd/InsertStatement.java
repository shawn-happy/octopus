package io.github.octopus.actus.core.model.curd;

import io.github.octopus.actus.core.model.schema.TablePath;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InsertStatement {
  private TablePath tablePath;
  private List<String> columns;
  private List<Object[]> values;
}
