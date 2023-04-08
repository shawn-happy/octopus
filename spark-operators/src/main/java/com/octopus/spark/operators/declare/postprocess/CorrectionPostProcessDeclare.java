package com.octopus.spark.operators.declare.postprocess;

import com.octopus.spark.operators.declare.postprocess.CorrectionPostProcessDeclare.CorrectionPostProcessOptions;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class CorrectionPostProcessDeclare
    implements PostProcessDeclare<CorrectionPostProcessOptions> {

  @Default private final PostProcessType postProcessType = PostProcessType.correction;
  private String name;
  private CorrectionPostProcessOptions options;
  private Alarm alarm;

  @Builder
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class CorrectionPostProcessOptions implements PostProcessOptions {
    private CorrectionPostProcessMode mode;
    private String sql;
    private String source;

    @Override
    public Map<String, String> getOptions() {
      return null;
    }

    @Override
    public void verify() {
      if (StringUtils.isEmpty(source)) {
        throw new IllegalArgumentException(
            "source can not be empty or null in correction post process transform com.octopus.spark.operators");
      }

      if (StringUtils.isEmpty(sql)) {
        throw new IllegalArgumentException(
            "sql can not be empty or null in correction post process transform com.octopus.spark.operators");
      }
    }
  }
}
