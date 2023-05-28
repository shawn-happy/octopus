package com.octopus.kettlex.core.steps.reader.rowgenerator;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.core.row.RecordExchanger;
import com.octopus.kettlex.core.row.column.Column;
import com.octopus.kettlex.core.row.record.DefaultRecord;
import com.octopus.kettlex.core.steps.Reader;
import com.octopus.kettlex.core.steps.common.Field;
import lombok.Getter;

@Getter
public class RowGenerator implements Reader<RowGeneratorConfig, RowGeneratorContext> {

  private final RowGeneratorConfig stepConfig;
  private final RowGeneratorContext stepContext;

  public RowGenerator(RowGeneratorConfig stepConfig, RowGeneratorContext stepContext) {
    this.stepConfig = stepConfig;
    this.stepContext = stepContext;
  }

  @Override
  public boolean init() throws KettleXException {
    try {
      stepConfig.verify();
      Field[] fields = stepConfig.getFields();

    } catch (Exception e) {
      throw new KettleXStepExecuteException("row generator init error", e);
    }
    return true;
  }

  @Override
  public void read(RecordExchanger recordExchanger) throws KettleXStepExecuteException {
    Field[] fields = stepConfig.getFields();
    Record record = new DefaultRecord();
    for (Field field : fields) {
      record.addColumn(
          Column.builder()
              .name(field.getName())
              .type(field.getFieldType())
              .rawData(field.getValue())
              .build());
    }
    recordExchanger.send(record);
  }

  @Override
  public int order() {
    return 0;
  }

  @Override
  public void destory() throws KettleXException {}
}
