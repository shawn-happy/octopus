package io.github.octopus.sql.executor.core.model;

import lombok.Getter;

@Getter
public enum FieldIdeEnum {
  ORIGINAL("original"), // Original string form
  UPPERCASE("uppercase"), // Convert to uppercase
  LOWERCASE("lowercase"); // Convert to lowercase

  private final String value;

  FieldIdeEnum(String value) {
    this.value = value;
  }
}
