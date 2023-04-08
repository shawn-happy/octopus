package com.octopus.spark.operators.declare.check;

import com.octopus.spark.operators.declare.check.CheckTransformDeclare.CheckTransformOptions;
import com.octopus.spark.operators.declare.common.Options;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class CheckTransformDeclare {

  private CheckTransformOptions options;
  private String name;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CheckTransformOptions implements Options {
    private Map<String, String> input;
    private String output;
    private Integer repartition;
    private String expression;
    private CheckLevel level;

    @Override
    public Map<String, String> getOptions() {
      return null;
    }

    @Override
    public void verify() {
      if (MapUtils.isEmpty(input)) {
        throw new IllegalArgumentException(
            "input can not be empty or null in transform com.octopus.spark.operators");
      }

      if (StringUtils.isEmpty(expression)) {
        throw new IllegalArgumentException(
            "sql can not be empty or null in check transform com.octopus.spark.operators");
      }
    }
  }
}
