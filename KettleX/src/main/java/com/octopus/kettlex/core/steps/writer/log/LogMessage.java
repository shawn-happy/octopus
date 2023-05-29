package com.octopus.kettlex.core.steps.writer.log;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.core.row.RecordExchanger;
import com.octopus.kettlex.core.row.column.Column;
import com.octopus.kettlex.core.steps.Writer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@AllArgsConstructor
public class LogMessage implements Writer<LogMessageMeta> {

  private final LogMessageMeta stepConfig;

  private static final String CR = System.getProperty("line.separator");

  @Override
  public boolean init() throws KettleXException {
    return true;
  }

  @Override
  public void destory() throws KettleXException {}

  @Override
  public void writer(RecordExchanger recordExchanger) throws KettleXStepExecuteException {
    Record record = recordExchanger.fetch();
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
