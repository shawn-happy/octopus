package com.octopus.kettlex.core.provider;

import java.util.List;

public interface StepConfigStepCombinationsBuilder {

  StepConfigStepCombinationsBuilder addBuiltInStepConfigSteps();

  StepConfigStepCombinationsBuilder addDiscoveredStepConfigSteps();

  List<StepConfigStepCombination> build();
}
