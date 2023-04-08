package com.octopus.spark.operators.runtime.step.transform.check;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.octopus.spark.operators.declare.check.CheckLevel;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class CheckResult {

  private boolean pass;
  private CheckLevel level;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable @Setter
  private String postProcess;
}
