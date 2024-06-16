package com.octopus.datatunnel.connector.source;

import com.octopus.datatunnel.connector.source.FakeSourceConfig.FakeSourceOptions;
import com.octopus.operators.engine.config.step.SourceConfig;
import com.octopus.operators.engine.config.step.StepConfig;
import com.octopus.operators.engine.config.step.StepOptions;
import com.octopus.operators.engine.exception.ConfigParseException;
import com.octopus.operators.engine.table.catalog.Column;
import com.octopus.operators.engine.util.JsonUtils;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FakeSourceConfig implements SourceConfig<FakeSourceOptions> {
  @Default private String type = "fake";
  private String id;
  private String name;
  private String description;
  private FakeSourceOptions options;
  private String resultTable;
  private List<Column> columns;
  @Default private Integer parallelism = 1;

  @Override
  public StepConfig<FakeSourceOptions> loadYaml(String yaml) {
    return null;
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class FakeSourceOptions implements StepOptions {

    @Default private int rowNum = 1000;
    private FakeSourceRow[] fields;

    public String[] getFieldNames() {
      if (ArrayUtils.isEmpty(fields)) {
        return null;
      }
      String[] fieldNames = new String[fields.length];
      for (int i = 0; i < fields.length; i++) {
        fieldNames[i] = fields[i].getFieldName();
      }
      return fieldNames;
    }

    public String[] getFieldTypes() {
      if (ArrayUtils.isEmpty(fields)) {
        return null;
      }
      String[] fieldTypes = new String[fields.length];
      for (int i = 0; i < fields.length; i++) {
        fieldTypes[i] = fields[i].getFieldType();
      }
      return fieldTypes;
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

    @Override
    public StepOptions loadYaml(String yaml) {
      return null;
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
