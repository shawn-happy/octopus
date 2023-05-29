package com.octopus.kettlex.model;

public interface WriterConfig<P extends WriterOptions> extends StepConfig {

  P getOptions();

  String getInput();

  WriteMode getWriteMode();
}
