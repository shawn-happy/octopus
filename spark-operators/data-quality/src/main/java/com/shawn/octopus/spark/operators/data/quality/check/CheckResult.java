package com.shawn.octopus.spark.operators.data.quality.check;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.shawn.octopus.spark.operators.common.declare.transform.check.CheckLevel;
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
  @Nullable
  @Setter
  private String postProcess;
}
