package com.octopus.operators.engine.config.step;

import com.octopus.operators.engine.exception.EngineException;
import java.util.Arrays;

public enum WriteMode {
  APPEND,
  REPLACE,
  OVERWRITE,
  ;

  public static WriteMode of(String mode) {
    return Arrays.stream(values())
        .filter(writeMode -> writeMode.name().equalsIgnoreCase(mode))
        .findFirst()
        .orElseThrow(() -> new EngineException("This WriteMode: " + mode + " is not supported"));
  }
}
