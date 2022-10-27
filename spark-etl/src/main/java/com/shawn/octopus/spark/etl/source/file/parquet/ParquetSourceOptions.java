package com.shawn.octopus.spark.etl.source.file.parquet;

import com.shawn.octopus.spark.etl.core.common.ColumnDesc;
import com.shawn.octopus.spark.etl.core.common.TableDesc;
import com.shawn.octopus.spark.etl.source.SourceOptions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

public class ParquetSourceOptions implements SourceOptions {

  private String[] paths;
  private String pathGlobFilter;
  private Boolean recursiveFileLookup = Boolean.TRUE;
  private int repartition;

  private List<ColumnDesc> columns;
  private List<TableDesc> outputs;

  private ParquetSourceOptions() {}

  @Override
  public int getRePartitions() {
    return repartition;
  }

  @Override
  public List<TableDesc> getOutputs() {
    return outputs;
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
    private List<TableDesc> outputs;
    private int repartition;

    public ParquetSourceOptionsBuilder pathGlobFilter(String pathGlobFilter) {
      this.pathGlobFilter = pathGlobFilter;
      return this;
    }

    public ParquetSourceOptionsBuilder repartition(int repartition) {
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

    public ParquetSourceOptionsBuilder outputs(List<TableDesc> outputs) {
      this.outputs = outputs;
      return this;
    }

    public ParquetSourceOptions build() {
      valid();
      ParquetSourceOptions parquetSourceOptions = new ParquetSourceOptions();
      parquetSourceOptions.paths = paths;
      parquetSourceOptions.repartition = repartition;
      parquetSourceOptions.pathGlobFilter = pathGlobFilter;
      parquetSourceOptions.recursiveFileLookup = recursiveFileLookup;
      parquetSourceOptions.outputs = outputs;
      parquetSourceOptions.columns = schemas;
      return parquetSourceOptions;
    }

    private void valid() {
      if (ArrayUtils.isEmpty(paths)) {
        throw new IllegalArgumentException("paths can not be empty or null");
      }
      if (CollectionUtils.isEmpty(outputs)) {
        throw new IllegalArgumentException("outputs can not be empty or null");
      }
      if (repartition < 0) {
        throw new IllegalArgumentException("repartition can not be less than 0");
      }
    }
  }
}
