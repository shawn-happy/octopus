package com.shawn.octopus.spark.operators.common.declare.transform.postprocess;

import com.shawn.octopus.spark.operators.common.SupportedTransformType;
import com.shawn.octopus.spark.operators.common.declare.transform.TransformOptions;
import com.shawn.octopus.spark.operators.common.declare.transform.postprocess.CorrectionPostProcessDeclare.CorrectionTransformOptions;
import java.util.Collections;
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
    implements PostProcessTransformDeclare<CorrectionTransformOptions> {

  @Default private final SupportedTransformType type = SupportedTransformType.postProcess;
  @Default private final PostProcessType postProcessType = PostProcessType.correction;
  private String name;
  private CorrectionTransformOptions options;
  private Alarm alarm;

  @Builder
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class CorrectionTransformOptions implements TransformOptions {
    private String output;
    private Integer repartition;
    private PostProcessCorrectionMode mode;
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
            "source can not be empty or null in correction post process transform operators");
      }

      if (StringUtils.isEmpty(sql)) {
        throw new IllegalArgumentException(
            "sql can not be empty or null in correction post process transform operators");
      }
    }

    @Override
    public Map<String, String> getInput() {
      return Collections.singletonMap(source, source);
    }
  }
}
