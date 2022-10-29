package com.shawn.octopus.spark.etl.source.file.parquet;

import com.shawn.octopus.spark.etl.core.common.ColumnDesc;
import com.shawn.octopus.spark.etl.source.SourceOptions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

public class ParquetSourceOptions implements SourceOptions {

  private String[] paths;
  private String pathGlobFilter;
  private Boolean recursiveFileLookup = Boolean.TRUE;
  private Integer repartition;

  private List<ColumnDesc> columns;
  private String output;

  private ParquetSourceOptions() {}

  @Override
  public Integer getRePartition() {
    return repartition;
  }

  @Override
  public String output() {
    return output;
  }

  public String[] getPaths() {
    return paths;
  }

  public List<ColumnDesc> getColumns() {
    return columns;
  }

  @Override
  public Map<String, String> getOptions() {
    Map<String, String> options = new HashMap<>();
    if (StringUtils.isNotBlank(pathGlobFilter)) {
      options.put("pathGlobFilter", pathGlobFilter);
    }
    options.put("recursiveFileLookup", String.valueOf(recursiveFileLookup));

    return options;
  }

  public static ParquetSourceOptionsBuilder builder() {
    return new ParquetSourceOptionsBuilder();
  }

  public static class ParquetSourceOptionsBuilder {
    private String pathGlobFilter;
    private boolean recursiveFileLookup = true;
    private String[] paths;
    private List<ColumnDesc> schemas;
    private String output;
    private Integer repartition;

    public ParquetSourceOptionsBuilder pathGlobFilter(String pathGlobFilter) {
      this.pathGlobFilter = pathGlobFilter;
      return this;
    }

    public ParquetSourceOptionsBuilder repartition(Integer repartition) {
      this.repartition = repartition;
      return this;
    }

    public ParquetSourceOptionsBuilder recursiveFileLookup(boolean recursiveFileLookup) {
      this.recursiveFileLookup = recursiveFileLookup;
      return this;
    }

    public ParquetSourceOptionsBuilder paths(String[] paths) {
      this.paths = paths;
      return this;
    }

    public ParquetSourceOptionsBuilder schemas(List<ColumnDesc> schemas) {
      this.schemas = schemas;
      return this;
    }

    public ParquetSourceOptionsBuilder output(String output) {
      this.output = output;
      return this;
    }

    public ParquetSourceOptions build() {
      valid();
      ParquetSourceOptions parquetSourceOptions = new ParquetSourceOptions();
      parquetSourceOptions.paths = paths;
      parquetSourceOptions.repartition = repartition;
      parquetSourceOptions.pathGlobFilter = pathGlobFilter;
      parquetSourceOptions.recursiveFileLookup = recursiveFileLookup;
      parquetSourceOptions.output = output;
      parquetSourceOptions.columns = schemas;
      return parquetSourceOptions;
    }

    private void valid() {
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
