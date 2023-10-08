package com.octopus.actus.connector.jdbc.model;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatabaseInfo {
  private String name;
  // for mysql
  private String charsetName;
  private String collationName;

  // for doris
  private Map<String, String> properties;
}
