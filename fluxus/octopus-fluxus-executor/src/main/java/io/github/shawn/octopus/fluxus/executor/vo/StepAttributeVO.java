package io.github.shawn.octopus.fluxus.executor.vo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StepAttributeVO {
  private String attributeId;
  private String code;
  private Object value;
}
