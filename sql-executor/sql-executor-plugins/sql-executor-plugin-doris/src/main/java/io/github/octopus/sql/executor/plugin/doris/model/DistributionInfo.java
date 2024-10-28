package io.github.octopus.sql.executor.plugin.doris.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DistributionInfo {
  private List<String> columns;
  private DistributionAlgo distributionAlgo;
  private int num;
}
