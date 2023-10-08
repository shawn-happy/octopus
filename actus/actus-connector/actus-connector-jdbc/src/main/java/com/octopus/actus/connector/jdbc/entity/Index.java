package com.octopus.actus.connector.jdbc.entity;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Index {
  private String indexName;
  private String algo;
  private List<String> columns;
  private String comment;
}
