package com.octopus.operators.spark.declare.postprocess;

import com.octopus.operators.spark.declare.postprocess.AlarmPostProcessDeclare.AlarmPostProcessOptions;
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
public class AlarmPostProcessDeclare implements PostProcessDeclare<AlarmPostProcessOptions> {

  @Default private PostProcessType type = PostProcessType.alarm;
  private String name;
  private AlarmPostProcessOptions options;

  @Builder
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class AlarmPostProcessOptions implements PostProcessOptions {
    private String source;

    @Override
    public Map<String, String> getOptions() {
      return Collections.emptyMap();
    }

    @Override
    public void verify() {
      Verify.verify(
          StringUtils.isNotBlank(source), "source can not be empty or null in alarm post process");
    }
  }
}
