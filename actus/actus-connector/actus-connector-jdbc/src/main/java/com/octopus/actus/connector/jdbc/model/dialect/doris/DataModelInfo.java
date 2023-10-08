package com.octopus.actus.connector.jdbc.model.dialect.doris;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataModelInfo {
  private AggregateType aggregateType;
  private List<String> columns;
}
