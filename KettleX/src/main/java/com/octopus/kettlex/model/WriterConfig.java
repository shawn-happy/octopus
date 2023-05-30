package com.octopus.kettlex.model;

public interface WriterConfig<P extends WriterOptions> extends StepConfig<P> {

  String getInput();

  WriteMode getWriteMode();
}
