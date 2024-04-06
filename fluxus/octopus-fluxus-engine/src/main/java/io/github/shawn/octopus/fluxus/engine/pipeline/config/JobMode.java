package io.github.shawn.octopus.fluxus.engine.pipeline.config;

import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import java.util.Arrays;

public enum JobMode {
  BATCH,
  STREAMING,
  ;

  public static JobMode of(String type) {
    return Arrays.stream(values())
        .filter(jobMode -> jobMode.name().equalsIgnoreCase(type))
        .findFirst()
        .orElseThrow(
            () -> new DataWorkflowException(String.format("job mode [%s] is not supported", type)));
  }
}
