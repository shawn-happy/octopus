package com.octopus.operators.engine.config.transform;

public interface TransformOptions {

  TransformOptions toOptions(String json);

  String toJson();
}
