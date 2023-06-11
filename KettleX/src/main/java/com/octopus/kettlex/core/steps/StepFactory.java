package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.model.StepConfig;
import com.octopus.kettlex.runtime.StepConfigChannelCombination;

public interface StepFactory {

  Step<? extends StepConfig<?>> createStep(StepConfigChannelCombination combination);
}
