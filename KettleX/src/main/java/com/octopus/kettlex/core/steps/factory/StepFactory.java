package com.octopus.kettlex.core.steps.factory;

import com.octopus.kettlex.core.steps.Step;
import com.octopus.kettlex.core.steps.StepContext;
import com.octopus.kettlex.core.steps.StepMeta;
import com.octopus.kettlex.core.steps.StepType;

public interface StepFactory<SM extends StepMeta, SC extends StepContext> {

  static Step createStep(StepMeta sm, StepContext sc) {
    StepType stepType = sm.getStepType();
    return null;
  }
}
