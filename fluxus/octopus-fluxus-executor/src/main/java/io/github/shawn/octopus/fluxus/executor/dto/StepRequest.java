package io.github.shawn.octopus.fluxus.executor.dto;

import io.github.shawn.octopus.fluxus.executor.bo.StepOptions;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StepRequest {
  private String name;
  private StepOptions options;
  private List<String> input;
  private String output;
}
