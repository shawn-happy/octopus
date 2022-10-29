package com.shawn.octopus.spark.etl.sink.file.csv;

import com.shawn.octopus.spark.etl.core.enums.WriteMode;
import com.shawn.octopus.spark.etl.sink.SinkOptions;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class CSVSinkOptions implements SinkOptions {

  private String input;
  private String filePath;
  private String encoding;
  private String dateFormat;
  private String timestampFormat;
  private Boolean hasHeader;
  private WriteMode writeMode;

  private CSVSinkOptions() {}

  @Override
  public Map<String, String> getOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("header", String.valueOf(hasHeader));
    options.put("encoding", encoding);
    options.put("dateFormat", dateFormat);
    options.put("timestampFormat", timestampFormat);
    return options;
  }

  @Override
  public String input() {
    return input;
  }

  @Override
  public WriteMode getWriteMode() {
    return writeMode;
  }

  public String getFilePath() {
    return filePath;
  }

  public Boolean getHasHeader() {
    return hasHeader;
  }

  public String getEncoding() {
    return encoding;
  }

  public String getDateFormat() {
    return dateFormat;
  }

  public String getTimestampFormat() {
    return timestampFormat;
  }

  public static CSVSinkOptionsBuilder builder() {
    return new CSVSinkOptionsBuilder();
  }

  public static class CSVSinkOptionsBuilder {
    private String input;
    private String filePath;
    private String encoding = "UTF-8";
    private String dateFormat = "yyyy-MM-dd";
    private String timestampFormat = "yyyy-MM-dd HH:mm:ss.SSS";
    private Boolean hasHeader = true;
    private WriteMode writeMode = WriteMode.append;

    public CSVSinkOptionsBuilder input(String input) {
      this.input = input;
      return this;
    }

    public CSVSinkOptionsBuilder dateFormat(String dateFormat) {
      this.dateFormat = dateFormat;
      return this;
    }

    public CSVSinkOptionsBuilder timestampFormat(String timestampFormat) {
      this.timestampFormat = timestampFormat;
      return this;
    }

    public CSVSinkOptionsBuilder encoding(String encoding) {
      this.encoding = encoding;
      return this;
    }

    public CSVSinkOptionsBuilder filePath(String filePath) {
      this.filePath = filePath;
      return this;
    }

    public CSVSinkOptionsBuilder header(Boolean hasHeader) {
      this.hasHeader = hasHeader;
      return this;
    }

    public CSVSinkOptionsBuilder writeMode(WriteMode writeMode) {
      this.writeMode = writeMode;
      return this;
    }

    public CSVSinkOptions build() {
      CSVSinkOptions options = new CSVSinkOptions();
      options.writeMode = writeMode;
      options.filePath = filePath;
      options.encoding = encoding;
      options.dateFormat = dateFormat;
      options.timestampFormat = timestampFormat;
      options.hasHeader = hasHeader;
      options.input = input;
      return options;
    }

    private void valid() {
      if (StringUtils.isEmpty(input)) {
        throw new IllegalArgumentException("sink operator must have input data");
      }
      if (StringUtils.isBlank(filePath)) {
        throw new IllegalArgumentException("csv sink operator must have file path");
      }
    }
  }
}
