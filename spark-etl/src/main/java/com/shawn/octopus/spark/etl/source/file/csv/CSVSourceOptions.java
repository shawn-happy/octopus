package com.shawn.octopus.spark.etl.source.file.csv;

import static com.shawn.octopus.spark.etl.core.util.ETLUtils.columnDescToSchema;

import com.shawn.octopus.spark.etl.core.common.ColumnDesc;
import com.shawn.octopus.spark.etl.core.enums.ReadParseErrorPolicy;
import com.shawn.octopus.spark.etl.source.SourceOptions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.types.StructType;

public class CSVSourceOptions implements SourceOptions {

  private String[] paths;
  private String pathGlobFilter;
  private boolean recursiveFileLookup;
  private Integer repartition;

  private boolean header;
  private String encoding;
  private String nullValue;
  private String nanValue;
  private String dateFormat;
  private String dateTimeFormat;
  private ReadParseErrorPolicy parseErrorPolicy;
  private boolean inferSchema;

  private List<ColumnDesc> columns;
  private String output;

  private CSVSourceOptions() {}

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

    options.put("encoding", encoding);
    options.put("header", String.valueOf(header));
    if (StringUtils.isNotBlank(nullValue)) {
      options.put("nullValue", nullValue);
    }
    options.put("nanValue", nanValue);
    options.put("dateFormat", dateFormat);
    options.put("timestampFormat", dateTimeFormat);
    options.put("mode", parseErrorPolicy.name());
    StructType schema = columnDescToSchema(columns);
    if (schema == null || schema.isEmpty()) {
      options.put("inferSchema", String.valueOf(inferSchema));
    }
    return options;
  }

  public static CSVSourceOptionsBuilder builder() {
    return new CSVSourceOptionsBuilder();
  }

  public boolean isHeader() {
    return header;
  }

  public String getEncoding() {
    return encoding;
  }

  public String getNullValue() {
    return nullValue;
  }

  public String getNanValue() {
    return nanValue;
  }

  public String getDateFormat() {
    return dateFormat;
  }

  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  public ReadParseErrorPolicy getParseErrorPolicy() {
    return parseErrorPolicy;
  }

  public boolean isInferSchema() {
    return inferSchema;
  }

  public static class CSVSourceOptionsBuilder {
    private String pathGlobFilter;
    private boolean recursiveFileLookup = true;
    private String[] paths;
    private List<ColumnDesc> schemas;
    private String output;
    private Integer repartition;

    private boolean header = true;
    private String encoding = "UTF-8";
    private String nullValue;
    private String nanValue = "NaN";
    private String dateFormat = "yyyy-MM-dd";
    private String dateTimeFormat = "yyyy-MM-dd HH:mm:ss";
    private ReadParseErrorPolicy parseErrorPolicy = ReadParseErrorPolicy.PERMISSIVE;
    private boolean inferSchema = false;

    public CSVSourceOptionsBuilder pathGlobFilter(String pathGlobFilter) {
      this.pathGlobFilter = pathGlobFilter;
      return this;
    }

    public CSVSourceOptionsBuilder recursiveFileLookup(boolean recursiveFileLookup) {
      this.recursiveFileLookup = recursiveFileLookup;
      return this;
    }

    public CSVSourceOptionsBuilder paths(String[] paths) {
      this.paths = paths;
      return this;
    }

    public CSVSourceOptionsBuilder repartition(Integer repartition) {
      this.repartition = repartition;
      return this;
    }

    public CSVSourceOptionsBuilder schemas(List<ColumnDesc> schemas) {
      this.schemas = schemas;
      return this;
    }

    public CSVSourceOptionsBuilder output(String output) {
      this.output = output;
      return this;
    }

    public CSVSourceOptionsBuilder header(boolean header) {
      this.header = header;
      return this;
    }

    public CSVSourceOptionsBuilder encoding(String encoding) {
      this.encoding = encoding;
      return this;
    }

    public CSVSourceOptionsBuilder nullValue(String nullValue) {
      this.nullValue = nullValue;
      return this;
    }

    public CSVSourceOptionsBuilder nanValue(String nanValue) {
      this.nanValue = nanValue;
      return this;
    }

    public CSVSourceOptionsBuilder dateFormat(String dateFormat) {
      this.dateFormat = dateFormat;
      return this;
    }

    public CSVSourceOptionsBuilder dateTimeFormat(String dateTimeFormat) {
      this.dateTimeFormat = dateTimeFormat;
      return this;
    }

    public CSVSourceOptionsBuilder parseErrorPolicy(ReadParseErrorPolicy parseErrorPolicy) {
      this.parseErrorPolicy = parseErrorPolicy;
      return this;
    }

    public CSVSourceOptionsBuilder inferSchema(boolean inferSchema) {
      this.inferSchema = inferSchema;
      return this;
    }

    public CSVSourceOptions build() {
      valid();
      CSVSourceOptions csvFileSourceConfig = new CSVSourceOptions();
      csvFileSourceConfig.paths = paths;
      csvFileSourceConfig.repartition = repartition;
      csvFileSourceConfig.pathGlobFilter = pathGlobFilter;
      csvFileSourceConfig.recursiveFileLookup = recursiveFileLookup;
      csvFileSourceConfig.header = this.header;
      csvFileSourceConfig.encoding = this.encoding;
      csvFileSourceConfig.nullValue = this.nullValue;
      csvFileSourceConfig.nanValue = this.nanValue;
      csvFileSourceConfig.dateFormat = this.dateFormat;
      csvFileSourceConfig.dateTimeFormat = this.dateTimeFormat;
      csvFileSourceConfig.parseErrorPolicy = this.parseErrorPolicy;
      csvFileSourceConfig.inferSchema = this.inferSchema;
      csvFileSourceConfig.output = output;
      csvFileSourceConfig.columns = schemas;
      return csvFileSourceConfig;
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
