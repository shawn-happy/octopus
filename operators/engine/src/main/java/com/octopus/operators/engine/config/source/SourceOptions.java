package com.octopus.operators.engine.config.source;

public interface SourceOptions {

  SourceOptions toOptions(String json);

  String toJson();
}
