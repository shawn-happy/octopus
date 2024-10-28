package io.github.octopus.datos.centro.sql.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IndexInfo {
  private String name;
  private List<String> columns;
  private String comment;
  private IndexAlgo indexAlgo;
}
