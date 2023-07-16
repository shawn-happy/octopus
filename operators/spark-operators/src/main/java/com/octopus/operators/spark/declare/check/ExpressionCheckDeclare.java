package com.octopus.operators.spark.declare.check;

import com.octopus.operators.spark.declare.check.ExpressionCheckDeclare.ExpressionCheckOptions;
import com.octopus.operators.spark.declare.common.CheckType;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.shaded.com.google.common.base.Verify;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ExpressionCheckDeclare implements CheckDeclare<ExpressionCheckOptions> {

  @Default private CheckType type = CheckType.expression;
  private ExpressionCheckOptions options;
  private String name;
  private List<String> metrics;
  private String output;
  private Integer repartition;
  @Default private CheckLevel checkLevel = CheckLevel.warning;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ExpressionCheckOptions implements CheckOptions {
    private String expression;

    @Override
    public Map<String, String> getOptions() {
      return Collections.emptyMap();
    }

    @Override
    public void verify() {
      Verify.verify(
          StringUtils.isNotBlank(expression), "expression can not be empty or null in check");
    }
  }
}
