package com.octopus.operators.engine.connector.source;

import com.fasterxml.jackson.core.type.TypeReference;
import com.octopus.operators.engine.config.source.SourceConfig;
import com.octopus.operators.engine.config.source.SourceOptions;
import com.octopus.operators.engine.config.source.SourceType;
import com.octopus.operators.engine.connector.source.FakeSourceConfig.FakeSourceOptions;
import com.octopus.operators.engine.util.JsonUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FakeSourceConfig implements SourceConfig<FakeSourceOptions> {
  @Default private final SourceType type = SourceType.FAKE;
  private String name;
  private FakeSourceOptions options;
  private String output;
  @Default private Integer parallelism = 1;

  public static class FakeSourceOptions implements SourceOptions {

    @Override
    public FakeSourceOptions toOptions(String json) {
      return JsonUtils.fromJson(json, new TypeReference<FakeSourceOptions>() {}).orElse(null);
    }

    @Override
    public String toJson() {
      return JsonUtils.toJson(this).orElse(null);
    }
  }
}
