package io.github.octopus.actus.core.model;

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

  public String identifier(String identifier) {
    switch (this) {
      case LOWERCASE:
        return identifier.toLowerCase();
      case UPPERCASE:
        return identifier.toUpperCase();
      default:
        return identifier;
    }
  }
}
