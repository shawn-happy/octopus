package com.octopus.spark.operators.declare.sink;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.octopus.spark.operators.declare.common.SinkType;
import com.octopus.spark.operators.declare.common.WriteMode;
import com.octopus.spark.operators.declare.sink.IcebergSinkDeclare.IcebergSinkOptions;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.shaded.com.google.common.base.Verify;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class IcebergSinkDeclare implements SinkDeclare<IcebergSinkOptions> {

  @Default private final SinkType type = SinkType.iceberg;
  private IcebergSinkOptions options;
  private WriteMode writeMode;
  private String input;
  private String name;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class IcebergSinkOptions implements SinkOptions {
    private String catalog;
    private String namespace;
    private String table;

    private List<String> partitionBy;
    private String dateField;
    private String replaceRangeStart;
    private String replaceRangeEnd;
    private String[] partitionExpressions;
    private Properties tableProperties;

    @JsonIgnore
    public String getFullTableName() {
      return catalog + "." + namespace + "." + getBackQuotesTable();
    }

    @JsonIgnore
    public String getBackQuotesTable() {
      return table.contains("-") && !table.startsWith("`") ? "`" + table + "`" : table;
    }

    @Override
    public Map<String, String> getOptions() {
      return null;
    }

    @Override
    public void verify() {
      Verify.verify(
          StringUtils.isNotBlank(catalog), "catalog can not be empty or null in iceberg sink");
      Verify.verify(
          StringUtils.isNotBlank(namespace), "namespace can not be empty or null in iceberg sink");
      Verify.verify(
          StringUtils.isNotBlank(table), "table can not be empty or null in iceberg sink");
    }
  }
}
