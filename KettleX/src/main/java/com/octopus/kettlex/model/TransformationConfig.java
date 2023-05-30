package com.octopus.kettlex.model;

public interface TransformationConfig<P extends TransformationOptions> extends StepConfig<P> {

  String getInput();

  String getOutput();
}
