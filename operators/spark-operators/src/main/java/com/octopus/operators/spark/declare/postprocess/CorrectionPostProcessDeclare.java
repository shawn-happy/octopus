package com.octopus.operators.spark.declare.postprocess;

import com.octopus.operators.spark.declare.postprocess.CorrectionPostProcessDeclare.CorrectionPostProcessOptions;
import java.util.Collections;
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
@AllArgsConstructor
@NoArgsConstructor
public class CorrectionPostProcessDeclare
    implements PostProcessDeclare<CorrectionPostProcessOptions> {

  @Default private final PostProcessType type = PostProcessType.correction;
  private String name;
  private CorrectionPostProcessOptions options;

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
      return Collections.emptyMap();
    }

    @Override
    public void verify() {
      Verify.verify(
          StringUtils.isNotBlank(source),
          "source can not be empty or null in correction post process");
      Verify.verify(
          StringUtils.isNotBlank(sql), "sql can not be empty or null in correction post process");
    }
  }
}
