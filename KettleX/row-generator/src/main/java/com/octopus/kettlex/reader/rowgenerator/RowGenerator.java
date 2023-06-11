package com.octopus.kettlex.reader.rowgenerator;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.core.row.column.Column;
import com.octopus.kettlex.core.row.record.DefaultRecord;
import com.octopus.kettlex.core.steps.BaseReader;
import com.octopus.kettlex.reader.rowgenerator.RowGeneratorConfig.RowGeneratorOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class RowGenerator extends BaseReader<RowGeneratorConfig> {

  private RowGeneratorConfig stepConfig;
  private Integer rowLimit;

  public RowGenerator() {
    super();
  }

  @Override
  public void doInit(RowGeneratorConfig stepConfig) throws KettleXException {
    RowGeneratorOptions options = stepConfig.getOptions();
    Integer rowLimit = options.getRowLimit();
    if (rowLimit == null) {
      rowLimit = 1;
    }
    this.rowLimit = rowLimit;
  }

  @Override
  protected Supplier<List<Record>> doReader() {
    Field[] fields = stepConfig.getOptions().getFields();
    int i = 0;
    List<Record> records = new ArrayList<>(rowLimit);
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
      records.add(record);
      i++;
    }
    return () -> records;
  }
}
