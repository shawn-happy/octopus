package com.octopus.kettlex.model;

public interface TransformationConfig<P extends TransformationOptions> extends StepConfig {

  P getOptions();

  String getInput();

  String getOutput();
}
