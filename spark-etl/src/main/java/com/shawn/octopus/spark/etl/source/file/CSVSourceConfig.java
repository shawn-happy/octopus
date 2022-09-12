package com.shawn.octopus.spark.etl.source.file;

import com.shawn.octopus.spark.etl.core.Format;
import com.shawn.octopus.spark.etl.core.ReadParseErrorPolicy;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

public class CSVSourceConfig extends FileSourceConfig {

  private String pathGlobFilter;
  private boolean recursiveFileLookup;

  private boolean header;
  private String encoding;
  private String nullValue;
  private String nanValue;
  private String dateFormat;
  private String dateTimeFormat;
  private ReadParseErrorPolicy parseErrorPolicy;
  private boolean inferSchema;

  public CSVSourceConfig(String[] paths) {
    super(paths);
  }

  @Override
  public String getType() {
    return Format.csv.name();
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
    options.put("inferSchema", String.valueOf(inferSchema));
    return options;
  }

  public static CSVSourceConfigBuilder builder() {
    return new CSVSourceConfigBuilder();
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

  public static class CSVSourceConfigBuilder {
    private String pathGlobFilter;
    private boolean recursiveFileLookup = true;
    private String[] paths;

    private boolean header = true;
    private String encoding = "UTF-8";
    private String nullValue;
    private String nanValue = "NaN";
    private String dateFormat = "yyyy-MM-dd";
    private String dateTimeFormat = "yyyy-MM-dd HH:mm:ss";
    private ReadParseErrorPolicy parseErrorPolicy = ReadParseErrorPolicy.PERMISSIVE;
    private boolean inferSchema = false;

    public CSVSourceConfigBuilder pathGlobFilter(String pathGlobFilter) {
      this.pathGlobFilter = pathGlobFilter;
      return this;
    }

    public CSVSourceConfigBuilder recursiveFileLookup(boolean recursiveFileLookup) {
      this.recursiveFileLookup = recursiveFileLookup;
      return this;
    }

    public CSVSourceConfigBuilder paths(String[] paths) {
      this.paths = paths;
      return this;
    }

    public CSVSourceConfigBuilder header(boolean header) {
      this.header = header;
      return this;
    }

    public CSVSourceConfigBuilder encoding(String encoding) {
      this.encoding = encoding;
      return this;
    }

    public CSVSourceConfigBuilder nullValue(String nullValue) {
      this.nullValue = nullValue;
      return this;
    }

    public CSVSourceConfigBuilder nanValue(String nanValue) {
      this.nanValue = nanValue;
      return this;
    }

    public CSVSourceConfigBuilder dateFormat(String dateFormat) {
      this.dateFormat = dateFormat;
      return this;
    }

    public CSVSourceConfigBuilder dateTimeFormat(String dateTimeFormat) {
      this.dateTimeFormat = dateTimeFormat;
      return this;
    }

    public CSVSourceConfigBuilder parseErrorPolicy(ReadParseErrorPolicy parseErrorPolicy) {
      this.parseErrorPolicy = parseErrorPolicy;
      return this;
    }

    public CSVSourceConfigBuilder inferSchema(boolean inferSchema) {
      this.inferSchema = inferSchema;
      return this;
    }

    public CSVSourceConfig build() {
      valid();
      CSVSourceConfig csvFileSourceConfig = new CSVSourceConfig(paths);
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
      return csvFileSourceConfig;
    }

    private void valid() {
      if (ArrayUtils.isEmpty(paths)) {
        throw new IllegalArgumentException("paths can not be empty");
      }
    }
  }
}
