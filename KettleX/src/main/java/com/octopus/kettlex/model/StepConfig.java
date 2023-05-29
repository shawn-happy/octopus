package com.octopus.kettlex.model;

import com.octopus.kettlex.core.steps.StepType;

public interface StepConfig {

  String getId();

  String getName();

  StepType getType();
}
