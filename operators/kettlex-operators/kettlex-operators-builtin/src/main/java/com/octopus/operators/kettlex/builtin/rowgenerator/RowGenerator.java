package com.octopus.operators.kettlex.builtin.rowgenerator;

import com.octopus.operators.kettlex.builtin.rowgenerator.RowGeneratorConfig.RowGeneratorOptions;
import com.octopus.operators.kettlex.core.exception.KettleXException;
import com.octopus.operators.kettlex.core.row.Record;
import com.octopus.operators.kettlex.core.row.column.Column;
import com.octopus.operators.kettlex.core.row.record.DefaultRecord;
import com.octopus.operators.kettlex.core.steps.BaseReader;

public class RowGenerator extends BaseReader<RowGeneratorConfig> {

  private RowGeneratorConfig stepConfig;
  private Integer rowLimit;

  public RowGenerator() {
    super();
  }

  @Override
  public void doInit(RowGeneratorConfig stepConfig) throws KettleXException {
    this.stepConfig = stepConfig;
    RowGeneratorOptions options = stepConfig.getOptions();
    Integer rowLimit = options.getRowLimit();
    if (rowLimit == null) {
      rowLimit = 1;
    }
    this.rowLimit = rowLimit;
  }

  @Override
  protected void doReader() {
    Field[] fields = stepConfig.getOptions().getFields();
    int i = 0;
    while (i < rowLimit) {
      Record record = new DefaultRecord();
      for (Field field : fields) {
        record.addColumn(
            Column.builder()
                .name(field.getName())
                .type(field.getFieldType())
                .rawData(field.getValue())
                .build());
      }
      putRow(record);
      i++;
    }
  }
}
