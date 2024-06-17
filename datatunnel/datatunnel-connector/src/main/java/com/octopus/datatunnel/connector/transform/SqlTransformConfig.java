package com.octopus.datatunnel.connector.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.octopus.datatunnel.connector.transform.SqlTransformConfig.SqlTransformOptions;
import com.octopus.operators.engine.config.CheckResult;
import com.octopus.operators.engine.config.step.AbstractTransformConfig;
import com.octopus.operators.engine.config.step.StepOptions;
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
public class SqlTransformConfig extends AbstractTransformConfig<SqlTransformOptions> {

  @Default private String type = "sql";
  private SqlTransformOptions options;

  @Override
  protected SqlTransformOptions loadOptions(JsonNode options) {
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

    @Override
    public List<CheckResult> check() {
      return null;
    }
  }
}
