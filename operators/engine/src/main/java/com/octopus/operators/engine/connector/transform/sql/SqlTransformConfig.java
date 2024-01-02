package com.octopus.operators.engine.connector.transform.sql;

import com.fasterxml.jackson.core.type.TypeReference;
import com.octopus.operators.engine.config.transform.TransformConfig;
import com.octopus.operators.engine.config.transform.TransformOptions;
import com.octopus.operators.engine.config.transform.TransformType;
import com.octopus.operators.engine.connector.transform.sql.SqlTransformConfig.SqlTransformOptions;
import com.octopus.operators.engine.exception.ConfigParseException;
import com.octopus.operators.engine.util.JsonUtils;
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

  private String name;
  @Default private TransformType type = TransformType.SQL;
  private SqlTransformOptions options;
  private Integer parallelism;
  private List<String> inputs;
  private String output;

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class SqlTransformOptions implements TransformOptions {

    private String sql;

    @Override
    public TransformOptions toOptions(String json) {
      return JsonUtils.fromJson(json, new TypeReference<TransformOptions>() {})
          .orElseThrow(
              () ->
                  new ConfigParseException(
                      String.format(
                          "sql transform options parse from json error. json content: \n%s",
                          json)));
    }

    @Override
    public String toJson() {
      return JsonUtils.toJson(this)
          .orElseThrow(
              () ->
                  new ConfigParseException(
                      String.format(
                          "sql transform options parse to json error. options: \n%s", this)));
    }
  }
}
