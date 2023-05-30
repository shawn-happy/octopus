package com.octopus.kettlex.model;

import com.octopus.kettlex.core.steps.StepType;

public interface StepConfig<P extends Options> {

  String getId();

  String getName();

  StepType getType();

  P getOptions();
}
