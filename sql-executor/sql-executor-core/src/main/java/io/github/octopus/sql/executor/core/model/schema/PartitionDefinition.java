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
  private List<String> columns;
  private PartitionAlgo partitionAlgo;
  private List<RangePartitionDef> rangePartitions;
  private List<ListPartitionDef> listPartitionDefs;

  // partition p1 values less than MAXVALUE|()
  // partition p2 values [("", ""), (), ()]
  // FROM () to () INTERVAL 1 YEAR
  // FROM () to () INTERVAL 1
  @Getter
  @Builder
  @AllArgsConstructor
  public static class RangePartitionDef {
    private final PartitionOperator partitionOperator;
    private final String name;
    private final boolean maxValue;
    private final Object[] lefts;
    private final Object[] rights;
    private final Object[] maxValues;
    private final int interval;
    private final IntervalType intervalType;
  }

  // partition p1 values in ("","")
  // partition p2 values in (("", ""), ("",""))
  @Getter
  @Builder
  @AllArgsConstructor
  public static class ListPartitionDef {
    private final String name;
    private final List<Object[]> values;
  }
}
