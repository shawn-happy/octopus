package com.octopus.kettlex.model;

public interface ReaderConfig<P extends ReaderOptions> extends StepConfig<P> {

  String getOutput();
}
