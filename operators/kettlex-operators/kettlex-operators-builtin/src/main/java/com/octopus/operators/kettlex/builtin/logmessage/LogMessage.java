package com.octopus.operators.kettlex.builtin.logmessage;

import com.octopus.operators.kettlex.core.exception.KettleXException;
import com.octopus.operators.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.operators.kettlex.core.row.Record;
import com.octopus.operators.kettlex.core.row.column.Column;
import com.octopus.operators.kettlex.core.steps.BaseWriter;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LogMessage extends BaseWriter<LogMessageConfig> {

  private LogMessageConfig stepConfig;

  private static final String CR = System.getProperty("line.separator");

  public LogMessage() {}

  @Override
  protected void doInit(LogMessageConfig stepConfig) throws KettleXException {
    this.stepConfig = stepConfig;
  }

  @Override
  protected Consumer<Record> doWriter() throws KettleXStepExecuteException {
    return record -> {
      try {
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
      } catch (Exception e) {
        throw new KettleXStepExecuteException(e);
      }
    };
  }
}
