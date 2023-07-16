package com.octopus.operators.spark.declare.source;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.octopus.operators.spark.declare.common.SourceType;
import com.octopus.operators.spark.declare.source.IcebergSourceDeclare.IcebergSourceOptions;
import java.util.HashMap;
import java.util.Map;
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
public class IcebergSourceDeclare implements SourceDeclare<IcebergSourceOptions> {

  @Default private final SourceType type = SourceType.iceberg;
  private IcebergSourceOptions options;
  private String name;
  private String output;
  private Integer repartition;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class IcebergSourceOptions implements SourceOptions {
    private String catalog;
    private String namespace;
    private String table;

    private Integer startSnapshotId;
    private Integer endSnapshotId;

    @Override
    public Map<String, String> getOptions() {
      Map<String, String> options = new HashMap<>();
      if (startSnapshotId != null || endSnapshotId != null) {
        if (endSnapshotId != null && (startSnapshotId == null || startSnapshotId == 0L)) {

          options.put("snapshot-id", String.valueOf(endSnapshotId));
        } else if (endSnapshotId == null) {
          options.put("start-snapshot-id", String.valueOf(startSnapshotId));
        } else {
          options.put("start-snapshot-id", String.valueOf(startSnapshotId));
          options.put("end-snapshot-id", String.valueOf(endSnapshotId));
        }
      }
      return options;
    }

    @Override
    public void verify() {
      Verify.verify(
          StringUtils.isNotBlank(catalog), "catalog can not be empty or null in iceberg source");
      Verify.verify(
          StringUtils.isNotBlank(namespace),
          "namespace can not be empty or null in iceberg source");
      Verify.verify(
          StringUtils.isNotBlank(table), "table can not be empty or null in iceberg source");
    }

    @JsonIgnore
    public String getFullTableName() {
      return catalog + "." + namespace + "." + getBackQuotesTable();
    }

    @JsonIgnore
    public String getBackQuotesTable() {
      return table.contains("-") && !table.startsWith("`") ? "`" + table + "`" : table;
    }
  }
}
