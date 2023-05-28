package com.octopus.kettlex.core.steps;

import static com.octopus.kettlex.core.steps.StepType.PrimaryCategory.SINK;
import static com.octopus.kettlex.core.steps.StepType.PrimaryCategory.SOURCE;
import static com.octopus.kettlex.core.steps.StepType.PrimaryCategory.TRANSFORMATION;

public enum StepType {
  // source step
  ROW_GENERATOR(SOURCE),

  // transform step
  VALUE_MAPPER(TRANSFORMATION),

  // sink step
  LOG_MESSAGE(SINK),
  ;

  private final PrimaryCategory primaryCategory;

  StepType(PrimaryCategory primaryCategory) {
    this.primaryCategory = primaryCategory;
  }

  public PrimaryCategory getPrimaryCategory() {
    return primaryCategory;
  }

  public static enum PrimaryCategory {
    SOURCE,
    TRANSFORMATION,
    SINK,
    ;
  }
}
