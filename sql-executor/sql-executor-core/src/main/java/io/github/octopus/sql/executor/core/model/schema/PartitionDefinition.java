package io.github.octopus.sql.executor.core.model.schema;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PartitionDefinition {
  private String name;
  private List<String> columns;
  private PartitionAlgo partitionAlgo;
  private PartitionOperator partitionOperator;
  private List<Object[]> lefts;
  private List<Object[]> rights;
  private List<Object[]> maxValues;
  private int[] interval;
  private IntervalType[] intervalType;
}
