package com.shawn.octopus.spark.operators.common.declare.source;

import com.shawn.octopus.spark.operators.common.ColumnDesc;
import com.shawn.octopus.spark.operators.common.ReadParseErrorPolicy;
import com.shawn.octopus.spark.operators.common.SupportedSourceType;
import com.shawn.octopus.spark.operators.common.declare.source.CSVSourceDeclare.CSVSourceOptions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class CSVSourceDeclare implements SourceDeclare<CSVSourceOptions> {

  @Default private SupportedSourceType type = SupportedSourceType.csv;
  private CSVSourceOptions options;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CSVSourceOptions implements SourceOptions {

    private String pathGlobFilter;
    @Default private boolean recursiveFileLookup = true;
    private String[] paths;
    private List<ColumnDesc> schemas;

    @Default private boolean header = true;
    @Default private String encoding = "UTF-8";
    private String nullValue;
    @Default private String nanValue = "NaN";
    @Default private String dateFormat = "yyyy-MM-dd";
    @Default private String dateTimeFormat = "yyyy-MM-dd HH:mm:ss.SSS";
    @Default private ReadParseErrorPolicy parseErrorPolicy = ReadParseErrorPolicy.PERMISSIVE;
    @Default private boolean inferSchema = false;

    private String output;
    private Integer repartition;

    @Override
    public Map<String, String> getOptions() {
      Map<String, String> options = new HashMap<>();
      if (StringUtils.isNotBlank(pathGlobFilter)) {
        options.put("pathGlobFilter", pathGlobFilter);
      }
      options.put("recursiveFileLookup", String.valueOf(recursiveFileLookup));

      options.put("encoding", encoding);
      options.put("header", String.valueOf(header));
      if (StringUtils.isNotBlank(nullValue)) {
        options.put("nullValue", nullValue);
      }
      options.put("nanValue", nanValue);
      options.put("dateFormat", dateFormat);
      options.put("timestampFormat", dateTimeFormat);
      options.put("mode", parseErrorPolicy.name());
      if (CollectionUtils.isEmpty(schemas)) {
        options.put("inferSchema", String.valueOf(inferSchema));
      }
      return options;
    }

    @Override
    public void verify() {
      if (ArrayUtils.isEmpty(paths)) {
        throw new IllegalArgumentException("paths can not be empty or null");
      }
      if (StringUtils.isEmpty(output)) {
        throw new IllegalArgumentException("outputs can not be empty or null");
      }
      if (Objects.nonNull(repartition) && repartition < 0) {
        throw new IllegalArgumentException("repartition can not be less than 0");
      }
    }
  }
}
