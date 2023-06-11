package com.octopus.kettlex.steps;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.core.row.column.Column;
import com.octopus.kettlex.core.steps.BaseWriter;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LogMessage extends BaseWriter<LogMessageConfig> {

  private LogMessageConfig stepConfig;

  private static final String CR = System.getProperty("line.separator");

  public LogMessage() {}

  @Override
  protected void doInit(LogMessageConfig stepConfig) throws KettleXException {
    super.doInit(stepConfig);
    this.stepConfig = stepConfig;
  }

  @Override
  protected Consumer<Record> doWriter() {
    return record -> {
      StringBuilder builder = new StringBuilder();
      builder.append(CR);
      builder.append("------------------------------>");
      builder.append(CR);
      for (int i = 0; i < record.getColumnNumber(); i++) {
        Column column = record.getColumn(i);
        Object rawData = column.getRawData();
        String name = column.getName();
        builder.append(name).append(" = ").append(rawData).append(CR);
      }
      log.info("{}", builder.toString());
    };
  }
}
