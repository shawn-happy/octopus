package com.octopus.operators.engine.config.sink;

public interface SinkOptions {

  SinkOptions toOptions(String json);

  String toJson();
}
