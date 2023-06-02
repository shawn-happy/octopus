package com.octopus.kettlex.runtime.writer;

import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.core.row.column.Column;
import com.octopus.kettlex.core.steps.BaseStep;
import com.octopus.kettlex.core.steps.Writer;
import com.octopus.kettlex.model.writer.LogMessageConfig;
import com.octopus.kettlex.runtime.TaskGroup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class LogMessage extends BaseStep<LogMessageConfig> implements Writer<LogMessageConfig> {

  private final LogMessageConfig stepConfig;

  private static final String CR = System.getProperty("line.separator");

  public LogMessage(LogMessageConfig stepConfig, TaskGroup taskGroup) {
    super(stepConfig, taskGroup);
    this.stepConfig = stepConfig;
  }

  @Override
  public void writer() throws KettleXStepExecuteException {
    Record record = getRow();
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
  }
}
