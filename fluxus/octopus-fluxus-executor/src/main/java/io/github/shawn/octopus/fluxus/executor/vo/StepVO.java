package io.github.shawn.octopus.fluxus.executor.vo;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StepVO {
  private String stepId;
  private String stepName;
  private String type;
  private String identifier;
  private String description;
  private List<String> input;
  private String output;
  private List<StepAttributeVO> attributes;
}
