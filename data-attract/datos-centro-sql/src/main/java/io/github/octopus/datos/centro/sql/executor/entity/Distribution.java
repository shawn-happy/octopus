package io.github.octopus.datos.centro.sql.executor.entity;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Distribution {
  private String algo;
  private List<String> columns;
  private int num;
}
