package com.octopus.kettlex.core.steps;

public enum StepType {
  RDBMS_INPUT(PrimaryCategory.SOURCE, "TableInput");

  private final PrimaryCategory primaryCategory;
  private final String secondaryCategory;

  StepType(PrimaryCategory primaryCategory, String secondaryCategory) {
    this.primaryCategory = primaryCategory;
    this.secondaryCategory = secondaryCategory;
  }

  public PrimaryCategory getPrimaryCategory() {
    return primaryCategory;
  }

  public String getSecondaryCategory() {
    return secondaryCategory;
  }
}
