package com.octopus.operators.spark.declare.source;

import com.octopus.operators.spark.declare.common.ColumnDesc;
import com.octopus.operators.spark.declare.common.SourceType;
import com.octopus.operators.spark.declare.source.CSVSourceDeclare.CSVSourceOptions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.shaded.com.google.common.base.Verify;
import org.jetbrains.annotations.NotNull;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class CSVSourceDeclare implements SourceDeclare<CSVSourceOptions> {

  @Default private SourceType type = SourceType.csv;
  private CSVSourceOptions options;
  @NotNull private String name;
  @NotNull private String output;
  private Integer repartition;

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
      Verify.verify(ArrayUtils.isNotEmpty(paths), "paths can not be empty or null");
    }
  }
}
