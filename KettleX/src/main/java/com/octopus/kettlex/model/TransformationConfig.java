package com.octopus.kettlex.model;

import java.util.Set;

public interface TransformationConfig<P extends TransformationOptions> extends StepConfig {

  P getOptions();

  Set<String> getInputs();

  String getOutput();
}
