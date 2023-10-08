package com.octopus.actus.connector.jdbc.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PartitionInfo {
  private List<String> columns;
  private List<String> names;
  private PartitionAlgo partitionAlgo;
  private PartitionOperator partitionOperator;
  private List<Object[]> lefts;
  private List<Object[]> rights;
  private List<Object[]> maxValues;
  private int[] interval;
  private IntervalType[] intervalType;
}
