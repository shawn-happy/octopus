package io.github.shawn.octopus.fluxus.executor.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class JobConfigRequest {
  private StepRequest source;
  private List<StepRequest> transforms;
  private StepRequest sink;
}
