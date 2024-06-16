package com.octopus.datatunnel.connector.transform;

import com.octopus.datatunnel.connector.transform.SqlTransformConfig.SqlTransformOptions;
import com.octopus.operators.engine.config.step.StepConfig;
import com.octopus.operators.engine.config.step.StepOptions;
import com.octopus.operators.engine.config.step.TransformConfig;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SqlTransformConfig implements TransformConfig<SqlTransformOptions> {

  private String id;
  private String name;
  @Default private String type = "sql";
  private String description;
  private SqlTransformOptions options;
  private Integer parallelism;
  private List<String> sourceTables;
  private String resultTable;

  @Override
  public StepConfig<SqlTransformOptions> loadYaml(String yaml) {
    return null;
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class SqlTransformOptions implements StepOptions {

    private String sql;

    @Override
    public StepOptions loadYaml(String yaml) {
      return null;
    }
  }
}
