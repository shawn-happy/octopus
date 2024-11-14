package io.github.octopus.actus.core.model.schema;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TabletDefinition {
  private List<String> columns;
  private TabletAlgo algo;
  private Integer num;
}
