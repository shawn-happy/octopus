package io.github.octopus.sql.executor.core.entity;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Partition {
  private List<String> columns;

  private boolean lessThan;
  private List<LessThanPartition> lessThanPartitions;

  private boolean fixedRange;
  private List<FixedRangePartition> fixedRangePartitions;

  private boolean dateMultiRange;
  private List<DateMultiRangePartition> dateMultiRangePartitions;

  private boolean numericMultiRange;
  private List<NumericMultiRangePartition> numericMultiRangePartitions;

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class LessThanPartition {
    private String partitionName;
    private Object[] maxValues;
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class FixedRangePartition {
    private String partitionName;
    private Object[] lefts;
    private Object[] rights;
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class DateMultiRangePartition {
    private String[] from;
    private String[] to;
    private long interval;
    private String type;
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class NumericMultiRangePartition {
    private long[] from;
    private long[] to;
    private long interval;
  }
}
