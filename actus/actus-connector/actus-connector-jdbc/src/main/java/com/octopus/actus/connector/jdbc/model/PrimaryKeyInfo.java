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
public class PrimaryKeyInfo {
  private List<String> columns;
  private String name;
}
