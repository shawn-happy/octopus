package io.github.octopus.sql.executor.core.model.schema;

import lombok.Getter;

@Getter
public enum ConstraintType {
  CHECK_KEY("CHECK", "CK"),
  PRIMARY_KEY("PRIMARY KEY", "PK"),
  UNIQUE_KEY("UNIQUE", "UK"),
  FOREIGN_KEY("FOREIGN KEY", "FK"),
  NOT_NULL("NOT NULL", "NOT_NULL"),
  KEY("KEY", "KEY"),
  ;

  private final String type;
  private final String shortName;

  ConstraintType(String type, String shortName) {
    this.type = type;
    this.shortName = shortName;
  }
}
