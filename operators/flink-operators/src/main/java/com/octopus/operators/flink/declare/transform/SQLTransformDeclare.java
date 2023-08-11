package com.octopus.operators.flink.declare.transform;

import com.google.common.base.Verify;
import com.octopus.operators.flink.declare.common.TransformType;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SQLTransformDeclare
    implements TransformDeclare<SQLTransformDeclare.SQLTransformOptions> {

  @Builder.Default private TransformType type = TransformType.sql;
  private SQLTransformOptions options;
  private String name;
  @Setter private Map<String, String> input;
  private String output;
  private Integer repartition;

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class SQLTransformOptions implements TransformOptions {
    private String sql;

    @Override
    public void verify() {
      Verify.verify(StringUtils.isNotBlank(sql), "sql can not be empty or null");
    }
  }
}
