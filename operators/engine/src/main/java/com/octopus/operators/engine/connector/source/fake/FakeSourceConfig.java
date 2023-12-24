package com.octopus.operators.engine.connector.source.fake;

import com.fasterxml.jackson.core.type.TypeReference;
import com.octopus.operators.engine.config.source.SourceConfig;
import com.octopus.operators.engine.config.source.SourceOptions;
import com.octopus.operators.engine.config.source.SourceType;
import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig.FakeSourceOptions;
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
public class FakeSourceConfig implements SourceConfig<FakeSourceOptions> {
  @Default private SourceType type = SourceType.FAKE;
  private String name;
  private FakeSourceOptions options;
  private String output;
  @Default private Integer parallelism = 1;

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class FakeSourceOptions implements SourceOptions {

    @Default private int rowNum = 1000;
    private FakeSourceRow[] fields;

    @Override
    public FakeSourceOptions toOptions(String json) {
      return JsonUtils.fromJson(json, new TypeReference<FakeSourceOptions>() {})
          .orElseThrow(
              () ->
                  new ConfigParseException(
                      String.format(
                          "fake source options parse from json error. json content: \n%s", json)));
    }

    @Override
    public String toJson() {
      return JsonUtils.toJson(this)
          .orElseThrow(
              () ->
                  new ConfigParseException(
                      String.format(
                          "fake source options parse to json error. options: \n%s", this)));
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder("[");
      for (FakeSourceRow field : fields) {
        builder.append(field.toString());
      }
      builder.append("]");
      return String.format("fake source options. row num: %d, row options: %s", rowNum, builder);
    }
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class FakeSourceRow {
    private String fieldName;
    private String fieldType;

    private List<String> booleanTemplate;

    private List<Integer> tinyIntTemplate;
    @Default private Integer tinyIntMin = 0;
    @Default private Integer tinyIntMax = 127;

    @Default private Integer smallIntMin = 0;
    @Default private Integer smallIntMax = 32767;
    private List<Integer> smallIntTemplate;

    @Default private Integer intMin = 0;
    @Default private Integer intMax = Integer.MAX_VALUE;
    private List<Integer> intTemplate;

    @Default private Long bigIntMin = 0L;
    @Default private Long bigIntMax = Long.MAX_VALUE;
    private List<Long> bigIntTemplate;

    @Default private float floatMin = 0.0f;
    @Default private float floatMax = Float.MAX_VALUE;
    private List<Float> floatTemplate;

    @Default private double doubleMin = 0.0D;
    @Default private double doubleMax = Double.MAX_VALUE;
    private List<Double> doubleTemplate;

    @Default private Integer stringLength = 5;
    private List<String> stringTemplate;

    private Integer decimalPrecision;
    private Integer decimalScale;

    private Integer year;
    private Integer month;
    private Integer day;
    private Integer hours;
    private Integer minutes;
    private Integer seconds;

    private Integer arraySize;

    private Integer byteSize;

    private Integer mapSize;

    @Override
    public String toString() {
      return JsonUtils.toJson(this)
          .orElseThrow(() -> new ConfigParseException("fake row option parse to json error"));
    }
  }
}
